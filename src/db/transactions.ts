import type { ClientSession, Db, TransactionOptions } from 'mongodb';
import { toDomainError } from '../errors/domain-error';
import { recordDbTransactionMetric } from './observability';

export type DbSession = ClientSession | null;

type TransactionWork<T> = (session: DbSession) => Promise<T>;

type DbWithClient = Db & {
  client?: {
    startSession?: () => ClientSession;
  };
};

type TransactionRetryPolicy = {
  maxAttempts: number;
  baseBackoffMs: number;
  maxBackoffMs: number;
};

const DEFAULT_TRANSACTION_OPTIONS: TransactionOptions = {
  maxCommitTimeMS: 5_000,
};

const RETRY_MAX_ATTEMPTS_ENV = 'MERISTEM_DATABASE_TX_MAX_ATTEMPTS';
const RETRY_BASE_BACKOFF_ENV = 'MERISTEM_DATABASE_TX_BASE_BACKOFF_MS';
const RETRY_MAX_BACKOFF_ENV = 'MERISTEM_DATABASE_TX_MAX_BACKOFF_MS';

const DEFAULT_RETRY_POLICY: TransactionRetryPolicy = {
  maxAttempts: 3,
  baseBackoffMs: 25,
  maxBackoffMs: 500,
};

const toClampedInteger = (
  value: string | undefined,
  fallback: number,
  min: number,
  max: number,
): number => {
  const parsed = Number.parseInt(value ?? '', 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  const normalized = Math.trunc(parsed);
  if (normalized < min) {
    return min;
  }
  if (normalized > max) {
    return max;
  }
  return normalized;
};

const resolveRetryPolicy = (): TransactionRetryPolicy => {
  const maxAttempts = toClampedInteger(
    process.env[RETRY_MAX_ATTEMPTS_ENV],
    DEFAULT_RETRY_POLICY.maxAttempts,
    1,
    8,
  );
  const baseBackoffMs = toClampedInteger(
    process.env[RETRY_BASE_BACKOFF_ENV],
    DEFAULT_RETRY_POLICY.baseBackoffMs,
    0,
    2_000,
  );
  const maxBackoffMs = toClampedInteger(
    process.env[RETRY_MAX_BACKOFF_ENV],
    DEFAULT_RETRY_POLICY.maxBackoffMs,
    1,
    10_000,
  );
  return {
    maxAttempts,
    baseBackoffMs,
    maxBackoffMs: Math.max(baseBackoffMs, maxBackoffMs),
  };
};

type MongoLabeledError = {
  hasErrorLabel?: (label: string) => boolean;
  code?: number;
};

const RETRYABLE_TRANSACTION_CODES = new Set<number>([
  112, // WriteConflict
  251, // NoSuchTransaction
]);

const hasErrorLabel = (
  error: unknown,
  label: string,
): boolean =>
  typeof (error as MongoLabeledError)?.hasErrorLabel === 'function' &&
  Boolean((error as MongoLabeledError).hasErrorLabel?.(label));

const isRetryableTransactionError = (error: unknown): boolean => {
  if (hasErrorLabel(error, 'TransientTransactionError')) {
    return true;
  }
  if (hasErrorLabel(error, 'UnknownTransactionCommitResult')) {
    return true;
  }
  const code = (error as MongoLabeledError)?.code;
  return typeof code === 'number' && RETRYABLE_TRANSACTION_CODES.has(code);
};

const computeBackoffMs = (
  attempt: number,
  policy: TransactionRetryPolicy,
): number => {
  if (policy.baseBackoffMs <= 0) {
    return 0;
  }
  const factor = 2 ** Math.max(0, attempt - 1);
  return Math.min(policy.maxBackoffMs, policy.baseBackoffMs * factor);
};

const sleep = async (delayMs: number): Promise<void> => {
  if (delayMs <= 0) {
    return;
  }
  await new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
};

const resolveSessionFactory = (db: Db): (() => ClientSession) | null => {
  const candidate = (db as DbWithClient).client?.startSession;
  if (typeof candidate !== 'function') {
    return null;
  }
  return candidate;
};

export const runInTransaction = async <T>(
  db: Db,
  work: TransactionWork<T>,
  options: TransactionOptions = DEFAULT_TRANSACTION_OPTIONS,
): Promise<T> => {
  const createSession = resolveSessionFactory(db);
  if (!createSession) {
    return work(null);
  }

  const retryPolicy = resolveRetryPolicy();
  let lastError: unknown = null;

  for (let attempt = 1; attempt <= retryPolicy.maxAttempts; attempt += 1) {
    const session = createSession();
    const startedAt = Date.now();
    const result: { done: boolean; value?: T } = { done: false };

    try {
      await session.withTransaction(async () => {
        result.value = await work(session);
        result.done = true;
      }, options);

      if (!result.done) {
        throw toDomainError(
          new Error('TRANSACTION_ABORTED'),
          'TRANSACTION_ABORTED',
        );
      }

      recordDbTransactionMetric({
        status: 'success',
        durationMs: Date.now() - startedAt,
      });
      return result.value as T;
    } catch (error) {
      const retryable =
        attempt < retryPolicy.maxAttempts &&
        isRetryableTransactionError(error);
      recordDbTransactionMetric({
        status: retryable ? 'retry' : 'failed',
        durationMs: Date.now() - startedAt,
      });
      lastError = error;

      if (!retryable) {
        throw toDomainError(error, 'TRANSACTION_ABORTED');
      }
    } finally {
      await session.endSession();
    }

    const backoffMs = computeBackoffMs(attempt, retryPolicy);
    await sleep(backoffMs);
  }

  throw toDomainError(lastError, 'TRANSACTION_ABORTED');
};

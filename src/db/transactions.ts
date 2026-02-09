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

const DEFAULT_TRANSACTION_OPTIONS: TransactionOptions = {
  maxCommitTimeMS: 5_000,
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

  const session = createSession();
  const startedAt = Date.now();
  const result: { done: boolean; value?: T } = { done: false };

  try {
    await session.withTransaction(async () => {
      result.value = await work(session);
      result.done = true;
    }, options);

    if (!result.done) {
      throw new Error('TRANSACTION_ABORTED');
    }

    recordDbTransactionMetric({
      status: 'success',
      durationMs: Date.now() - startedAt,
    });
    return result.value as T;
  } catch (error) {
    recordDbTransactionMetric({
      status: 'failed',
      durationMs: Date.now() - startedAt,
    });
    throw toDomainError(error, 'TRANSACTION_ABORTED');
  } finally {
    await session.endSession();
  }
};

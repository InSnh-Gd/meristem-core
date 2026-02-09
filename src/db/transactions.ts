import type { ClientSession, Db, TransactionOptions } from 'mongodb';
import { toDomainError } from '../errors/domain-error';
import { recordDbTransactionMetric } from './observability';
import { getMongoClient } from './connection';

export type DbSession = ClientSession | null;

type TransactionWork<T> = (session: DbSession) => Promise<T>;

type DbWithClient = Db & {
  client?: {
    startSession?: () => ClientSession;
  };
};

type ManagedSession = {
  session: ClientSession;
  [Symbol.asyncDispose]: () => Promise<void>;
};

const DEFAULT_TRANSACTION_OPTIONS: TransactionOptions = {
  maxCommitTimeMS: 5_000,
};

const resolveSessionFactory = (db: Db): (() => ClientSession) | null => {
  const dbClient = (db as DbWithClient).client;
  if (dbClient && typeof dbClient.startSession === 'function') {
    // MongoDB driver methods rely on `this`; return a bound callable.
    return dbClient.startSession.bind(dbClient);
  }

  const sharedClient = getMongoClient();
  if (sharedClient && typeof sharedClient.startSession === 'function') {
    return sharedClient.startSession.bind(sharedClient);
  }

  return null;
};

const toManagedSession = (session: ClientSession): ManagedSession => ({
  session,
  [Symbol.asyncDispose]: async (): Promise<void> => {
    await session.endSession();
  },
});

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

const getStringField = (value: unknown, key: string): string | null => {
  if (!isRecord(value)) {
    return null;
  }
  const candidate = value[key];
  return typeof candidate === 'string' ? candidate : null;
};

const getNumericField = (value: unknown, key: string): number | null => {
  if (!isRecord(value)) {
    return null;
  }
  const candidate = value[key];
  return typeof candidate === 'number' ? candidate : null;
};

const isTransactionUnsupportedError = (error: unknown): boolean => {
  // Standalone MongoDB (non-replica-set) rejects transactions with IllegalOperation.
  const code = getNumericField(error, 'code');
  if (code === 20) {
    return true;
  }

  const codeName = getStringField(error, 'codeName');
  if (codeName === 'IllegalOperation') {
    return true;
  }

  const message = getStringField(error, 'message');
  if (
    message &&
    message.includes('Transaction numbers are only allowed on a replica set member or mongos')
  ) {
    return true;
  }

  const nestedMessage = getStringField(error, 'errmsg');
  if (
    nestedMessage &&
    nestedMessage.includes('Transaction numbers are only allowed on a replica set member or mongos')
  ) {
    return true;
  }

  return false;
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
  await using managedSession = toManagedSession(session);
  const startedAt = Date.now();
  const result: { done: boolean; value?: T } = { done: false };

  try {
    await managedSession.session.withTransaction(async () => {
      result.value = await work(managedSession.session);
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
    if (!result.done && isTransactionUnsupportedError(error)) {
      // Safe fallback: work was not committed inside a transaction.
      // We rerun once without session to keep non-replica-set environments functional.
      return work(null);
    }
    throw toDomainError(error, 'TRANSACTION_ABORTED');
  }
};

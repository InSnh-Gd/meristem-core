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
    throw toDomainError(error, 'TRANSACTION_ABORTED');
  }
};

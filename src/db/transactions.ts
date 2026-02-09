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

/**
 * 解析可用的 Session 工厂：
 * 1) 优先使用当前 Db 实例挂载的 client；
 * 2) 若当前 Db 未暴露 client，则回退到连接模块中的共享 client；
 * 3) 两种来源都必须返回 bind 后的方法，避免 Mongo 驱动因 this 丢失触发运行时异常。
 */
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

/**
 * 识别“事务能力不可用”错误（典型为 standalone Mongo）：
 * 该类错误不是业务失败，而是部署形态限制；后续会触发安全降级路径。
 */
const isTransactionUnsupportedError = (error: unknown): boolean => {
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
      /**
       * 事务不可用时的单次降级执行：
       * - 前置条件：事务体尚未成功完成（result.done=false）；
       * - 策略：仅重试一次无 session 路径，保证 standalone 环境可用；
       * - 边界：若事务体已完成或命中其他错误，仍按 TRANSACTION_ABORTED 抛出。
       */
      return work(null);
    }
    throw toDomainError(error, 'TRANSACTION_ABORTED');
  }
};

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

const MAX_TRANSIENT_RETRY_ATTEMPTS = 3;
const RETRY_BASE_DELAY_MS = 25;
const RETRY_MAX_DELAY_MS = 250;
const RETRY_JITTER_MS = 25;

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

const sleep = async (durationMs: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });

const hasErrorLabel = (error: unknown, label: string): boolean => {
  if (!isRecord(error)) {
    return false;
  }

  const hasErrorLabelField = error.hasErrorLabel;
  if (typeof hasErrorLabelField === 'function') {
    try {
      return Boolean(
        (hasErrorLabelField as (candidate: string) => boolean).call(error, label),
      );
    } catch {
      // ignore and fallback to errorLabels parsing.
    }
  }

  const errorLabelsField = error.errorLabels;
  if (Array.isArray(errorLabelsField)) {
    return errorLabelsField.some((candidate) => candidate === label);
  }

  return false;
};

const isTransientTransactionError = (error: unknown): boolean => {
  if (hasErrorLabel(error, 'TransientTransactionError')) {
    return true;
  }

  const codeName = getStringField(error, 'codeName');
  if (codeName === 'WriteConflict' || codeName === 'NoSuchTransaction') {
    return true;
  }

  const code = getNumericField(error, 'code');
  return code === 112 || code === 251;
};

const calculateRetryDelayMs = (attempt: number): number => {
  const exponentialStep = Math.min(2 ** Math.max(0, attempt - 1), 8);
  const baseDelay = RETRY_BASE_DELAY_MS * exponentialStep;
  const jitter = Math.floor(Math.random() * RETRY_JITTER_MS);
  return Math.min(RETRY_MAX_DELAY_MS, baseDelay + jitter);
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

  const startedAt = Date.now();
  let retryAttempt = 0;

  while (true) {
    const session = createSession();
    await using managedSession = toManagedSession(session);
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
      if (!result.done && isTransactionUnsupportedError(error)) {
        /**
         * 逻辑块：事务能力缺失直接失败，不再降级为无事务执行。
         * 原因：审计/权限等关键写路径依赖事务原子性，多写场景下降级会放大一致性风险。
         * 处置：让部署侧修复 Mongo 拓扑（ReplicaSet/Mongos），而不是在应用层吞掉约束。
         */
        recordDbTransactionMetric({
          status: 'failed',
          durationMs: Date.now() - startedAt,
        });
        throw toDomainError(error, 'TRANSACTION_ABORTED');
      }

      /**
       * 逻辑块：仅对“事务体尚未完成”的瞬时冲突做有限重试。
       * 这样可在不重复提交已完成业务逻辑的前提下吸收 Mongo 写冲突抖动；
       * 若超过上限仍失败，则保持 fail-fast，避免无限重试放大尾延迟。
       */
      if (
        !result.done &&
        isTransientTransactionError(error) &&
        retryAttempt < MAX_TRANSIENT_RETRY_ATTEMPTS
      ) {
        recordDbTransactionMetric({
          status: 'retry',
          durationMs: Date.now() - startedAt,
        });
        retryAttempt += 1;
        const retryDelayMs = calculateRetryDelayMs(retryAttempt);
        await sleep(retryDelayMs);
        continue;
      }

      recordDbTransactionMetric({
        status: 'failed',
        durationMs: Date.now() - startedAt,
      });
      throw toDomainError(error, 'TRANSACTION_ABORTED');
    }
  }
};

/**
 * 审计日志服务 (Audit Service)
 *
 * 使用 Hash-Chain 模式实现防篡改审计日志：
 * - 每条日志包含 _sequence（序列号）、_hash（当前哈希）、_previous_hash（前一条哈希）
 * - 通过链式哈希确保日志序列的完整性和不可篡改性
 * - 任何中间日志的修改都会导致后续所有哈希验证失败
 *
 * 为什么使用 Hash-Chain：
 * 1. 防篡改：修改任何一条日志都会破坏哈希链
 * 2. 完整性验证：可以快速检测日志是否被删除或插入
 * 3. 合规性：满足审计追踪的不可抵赖要求
 */

import { Db, Collection } from 'mongodb';
import { createHash } from 'crypto';

/**
 * 审计日志类型定义
 *
 * 基于 meristem-logs 规范的日志信封格式，扩展 hash-chain 字段
 */
export type AuditLog = {
  /**
   * Unix 毫秒时间戳
   */
  ts: number;

  /**
   * 日志级别
   */
  level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR' | 'FATAL';

  /**
   * 产生日志的节点 ID
   */
  node_id: string;

  /**
   * 模块名 (如: m-net, worker-vm, core)
   */
  source: string;

  /**
   * 全局链路追踪 ID
   */
  trace_id: string;

  /**
   * 日志正文
   */
  content: string;

  /**
   * 元数据（taskId, errorCode, actor, target 等）
   */
  meta: Record<string, unknown>;

  /**
   * 序列号（自增，确保日志顺序）
   */
  _sequence: number;

  /**
   * 当前日志的 SHA-256 哈希值
   */
  _hash: string;

  /**
   * 前一条日志的哈希值（首条日志为空字符串）
   */
  _previous_hash: string;

  /**
   * 幂等事件 ID（新流水线可选）
   */
  event_id?: string;

  /**
   * 链版本（兼容扩展）
   */
  chain_version?: number;

  /**
   * 分区链元数据（兼容扩展）
   */
  partition_id?: number;
  partition_sequence?: number;
  partition_hash?: string;
  partition_previous_hash?: string;
};

/**
 * 审计事件输入类型（不包含自动生成的字段）
 */
export type AuditEventInput = Omit<AuditLog, '_sequence' | '_hash' | '_previous_hash'>;

/**
 * 审计集合名称
 */
export const AUDIT_COLLECTION = 'audit_logs';
export const AUDIT_STATE_COLLECTION = 'audit_state';

/**
 * 审计序列状态文档主键
 */
const AUDIT_SEQUENCE_STATE_ID = 'global';

/**
 * 前驱日志等待策略
 *
 * 在高并发场景下，后续序号可能先于前驱完成插入。
 * 通过带退避的等待前驱落库，确保 _previous_hash 精确指向 sequence-1。
 */
const PREVIOUS_HASH_MAX_WAIT_MS = 30_000;
const PREVIOUS_HASH_INITIAL_RETRY_DELAY_MS = 2;
const PREVIOUS_HASH_MAX_RETRY_DELAY_MS = 50;

type AuditSequenceState = {
  _id: string;
  value: number;
};

const isDuplicateKeyError = (error: unknown): boolean => {
  if (typeof error !== 'object' || error === null) {
    return false;
  }
  const candidate = error as { code?: unknown };
  return candidate.code === 11000;
};

/**
 * 稳定 JSON 序列化函数
 *
 * 对对象进行 JSON 序列化，并按 key 排序以确保确定性
 * 相同的对象总是产生相同的 JSON 字符串
 *
 * @param obj - 要序列化的对象
 * @returns 确定性的 JSON 字符串
 */
const stableStringify = (obj: Record<string, unknown>): string => {
  const sortedKeys = Object.keys(obj).sort();
  const sortedObj: Record<string, unknown> = {};
  for (const key of sortedKeys) {
    sortedObj[key] = obj[key];
  }
  return JSON.stringify(sortedObj);
};

/**
 * 计算 SHA-256 哈希值
 *
 * 使用稳定 JSON 序列化确保相同输入产生相同哈希
 * 哈希计算包含所有审计日志字段（除 _hash 和 _previous_hash 外）
 *
 * @param log - 审计日志对象
 * @returns SHA-256 哈希值（十六进制字符串）
 */
export const calculateHash = (log: AuditLog): string => {
  // 提取需要哈希的字段（排除 _hash 和 _previous_hash）
  const hashInput: Record<string, unknown> = {
    ts: log.ts,
    level: log.level,
    node_id: log.node_id,
    source: log.source,
    trace_id: log.trace_id,
    content: log.content,
    meta: log.meta,
    _sequence: log._sequence,
    _previous_hash: log._previous_hash,
  };

  // 稳定序列化并计算 SHA-256
  const serialized = stableStringify(hashInput);
  return createHash('sha256').update(serialized).digest('hex');
};

/**
 * 验证哈希链完整性
 *
 * 检查日志序列的连续性和哈希链的正确性：
 * 1. 序列号必须连续递增
 * 2. 每条日志的 _previous_hash 必须等于前一条日志的 _hash
 * 3. 每条日志的 _hash 必须通过重新计算验证
 *
 * @param logs - 审计日志数组（必须按 _sequence 排序）
 * @returns 验证结果，包含 valid 标志和可选的错误信息
 */
export const verifyChain = (logs: AuditLog[]): { valid: boolean; error?: string } => {
  if (logs.length === 0) {
    return { valid: true };
  }

  // 检查序列号连续性
  for (let i = 0; i < logs.length; i++) {
    const expectedSequence = i + 1;
    if (logs[i]._sequence !== expectedSequence) {
      return {
        valid: false,
        error: `序列号不连续：期望 ${expectedSequence}，实际 ${logs[i]._sequence}`,
      };
    }
  }

  // 检查哈希链
  for (let i = 0; i < logs.length; i++) {
    const log = logs[i];

    // 验证当前日志的哈希
    const calculatedHash = calculateHash(log);
    if (log._hash !== calculatedHash) {
      return {
        valid: false,
        error: `哈希验证失败：序列 ${log._sequence} 的哈希不匹配`,
      };
    }

    // 验证前一条哈希链接（首条日志除外）
    if (i > 0) {
      const previousLog = logs[i - 1];
      if (log._previous_hash !== previousLog._hash) {
        return {
          valid: false,
          error: `哈希链断裂：序列 ${log._sequence} 的 _previous_hash 与前一条日志不匹配`,
        };
      }
    } else {
      // 首条日志的 _previous_hash 必须为空字符串
      if (log._previous_hash !== '') {
        return {
          valid: false,
          error: `首条日志的 _previous_hash 必须为空字符串`,
        };
      }
    }
  }

  return { valid: true };
};

const sleep = async (durationMs: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });

/**
 * 原子分配序列号
 *
 * 使用 findOneAndUpdate + $inc 保证并发下序号唯一且严格递增。
 */
const allocateSequence = async (db: Db): Promise<number> => {
  const auditCollection: Collection<AuditLog> = db.collection(AUDIT_COLLECTION);
  const stateCollection: Collection<AuditSequenceState> = db.collection(AUDIT_STATE_COLLECTION);

  const existingState = await stateCollection.findOne({ _id: AUDIT_SEQUENCE_STATE_ID });
  if (!existingState) {
    const latestLog = await auditCollection.findOne({}, { sort: { _sequence: -1 } });
    const latestSequence = latestLog?._sequence ?? 0;
    try {
      await stateCollection.insertOne({
        _id: AUDIT_SEQUENCE_STATE_ID,
        value: latestSequence,
      });
    } catch (error) {
      if (!isDuplicateKeyError(error)) {
        throw error;
      }
    }
  }

  const state = await stateCollection.findOneAndUpdate(
    { _id: AUDIT_SEQUENCE_STATE_ID },
    { $inc: { value: 1 } },
    { upsert: true, returnDocument: 'after' },
  );

  if (state === null || typeof state.value !== 'number') {
    throw new Error('audit sequence allocation failed');
  }

  return state.value;
};

const findLogHashBySequence = async (
  collection: Collection<AuditLog>,
  sequence: number,
): Promise<string | null> => {
  const previousLog = await collection.findOne(
    { _sequence: sequence },
    { projection: { _hash: 1 } },
  );
  return previousLog?._hash ?? null;
};

const waitForPreviousHash = async (
  collection: Collection<AuditLog>,
  sequence: number,
): Promise<string> => {
  if (sequence <= 1) {
    return '';
  }

  const predecessorSequence = sequence - 1;
  const startedAt = Date.now();
  let delayMs = PREVIOUS_HASH_INITIAL_RETRY_DELAY_MS;
  let attempts = 0;

  for (;;) {
    const previousHash = await findLogHashBySequence(collection, predecessorSequence);
    if (previousHash !== null) {
      return previousHash;
    }

    attempts += 1;
    const elapsedMs = Date.now() - startedAt;
    if (elapsedMs >= PREVIOUS_HASH_MAX_WAIT_MS) {
      break;
    }

    await sleep(delayMs);
    delayMs = Math.min(delayMs * 2, PREVIOUS_HASH_MAX_RETRY_DELAY_MS);
  }

  throw new Error(
    `audit predecessor missing: sequence=${sequence}, predecessor=${predecessorSequence}, elapsed_ms=${Date.now() - startedAt}, attempts=${attempts}`,
  );
};

/**
 * 记录审计事件
 *
 * 自动维护 _sequence 和 _previous_hash，确保哈希链完整性：
 * 1. 获取当前序列号并递增
 * 2. 查询前一条日志的哈希值
 * 3. 计算当前日志的哈希值
 * 4. 写入数据库
 *
 * @param db - MongoDB 数据库实例
 * @param event - 审计事件（不包含 _sequence、_hash、_previous_hash）
 * @returns 完整的审计日志对象（包含自动生成的字段）
 */
export const logAuditEvent = async (
  db: Db,
  event: AuditEventInput
): Promise<AuditLog> => {
  const collection: Collection<AuditLog> = db.collection(AUDIT_COLLECTION);
  const stateCollection: Collection<AuditSequenceState> = db.collection(AUDIT_STATE_COLLECTION);

  for (let attempt = 0; attempt < 2; attempt += 1) {
    // 原子获取序列号，并精确等待前驱日志落库
    const sequence = await allocateSequence(db);
    const previousHash = await waitForPreviousHash(collection, sequence);

    // 构建完整的审计日志
    const auditLog: AuditLog = {
      ...event,
      _sequence: sequence,
      _previous_hash: previousHash,
      _hash: '', // 稍后计算
    };

    // 计算哈希值
    auditLog._hash = calculateHash(auditLog);

    try {
      // 写入数据库
      await collection.insertOne(auditLog);
      return auditLog;
    } catch (error) {
      if (!isDuplicateKeyError(error) || attempt > 0) {
        throw error;
      }

      // 自愈：当 audit_state 落后于历史 audit_logs 时，先提升计数器再重试
      const latestLog = await collection.findOne({}, { sort: { _sequence: -1 } });
      const latestSequence = latestLog?._sequence ?? 0;
      await stateCollection.findOneAndUpdate(
        { _id: AUDIT_SEQUENCE_STATE_ID },
        { $max: { value: latestSequence } },
        { upsert: true },
      );
    }
  }

  throw new Error('audit write retry exhausted');
};

/**
 * 重置模块级状态（仅用于测试）
 *
 * 清除缓存的序列号，强制下次调用时重新从数据库读取
 */
export const resetAuditState = (): void => {
  // 仅保留测试兼容入口；当前实现不依赖模块级缓存状态
};

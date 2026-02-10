import { createHash, randomUUID } from 'crypto';
import type { ClientSession, Collection, Db } from 'mongodb';
import { DomainError } from '../errors/domain-error';
import type { DbSession } from '../db/transactions';
import { runInTransaction } from '../db/transactions';
import { createLogger, type Logger } from '../utils/logger';
import type { TraceContext } from '../utils/trace-context';
import {
  calculatePartitionHash,
  calculatePartitionId,
  calculatePayloadDigest,
  calculatePayloadHmac,
  stableStringifyUnknown,
} from './audit-pipeline-hash';
import {
  DEFAULT_OPTIONS,
  normalizeOptions,
  type AuditPipelineOptions,
  type AuditPipelineStartOptions,
} from './audit-pipeline-options';
import {
  AUDIT_COLLECTION,
  AUDIT_STATE_COLLECTION,
  calculateHash,
  logAuditEvent,
  type AuditEventInput,
  type AuditLog,
} from './audit';

export const AUDIT_INTENTS_COLLECTION = 'audit_intents';
export const AUDIT_PARTITION_STATE_COLLECTION = 'audit_partition_state';
export const AUDIT_GLOBAL_ANCHOR_COLLECTION = 'audit_global_anchor';
export const AUDIT_FAILURES_COLLECTION = 'audit_failures';

const AUDIT_STATE_ID = 'global';

type AuditIntentStatus =
  | 'pending'
  | 'processing'
  | 'ready_for_global_commit'
  | 'committed'
  | 'failed_retriable'
  | 'failed_terminal';

export type AuditIntent = {
  event_id: string;
  route_tag: string;
  partition_id: number;
  status: AuditIntentStatus;
  lease_owner: string | null;
  lease_until: Date | null;
  attempt_count: number;
  created_at: Date;
  updated_at: Date;
  payload: AuditEventInput;
  payload_digest: string;
  payload_hmac: string;
  hmac_key_id: string;
  global_sequence?: number;
  committed_at?: Date;
  error_last?: string;
};

export type AuditPartitionState = {
  _id: string;
  partition_id: number;
  last_sequence: number;
  last_hash: string;
  updated_at: Date;
};

export type AuditGlobalAnchorHead = {
  partition_id: number;
  last_sequence: number;
  last_hash: string;
};

export type AuditGlobalAnchor = {
  anchor_id: string;
  ts: number;
  partition_heads: AuditGlobalAnchorHead[];
  previous_anchor_hash: string;
  anchor_hash: string;
};

export type AuditFailureRecord = {
  event_id: string;
  route_tag: string;
  reason: string;
  payload_digest: string;
  payload_hmac: string;
  hmac_key_id: string;
  created_at: Date;
};

type BackpressureReason = 'backpressure' | 'pipeline_unavailable';

type IntakeResult = {
  accepted: boolean;
  reason?: BackpressureReason;
  retryAfterSeconds?: number;
};

type EnqueueOptions = {
  routeTag: string;
  session?: DbSession;
};

type RecordAuditOptions = {
  routeTag: string;
  session?: DbSession;
};

type GlobalTail = {
  sequence: number;
  hash: string;
};

type PartitionTail = {
  sequence: number;
  hash: string;
};

type AuditStateDocument = {
  _id: string;
  value: number;
  global_last_sequence?: number;
  global_last_hash?: string;
  updated_at?: Date;
  backpressure_soft_limit?: number;
  backpressure_hard_limit?: number;
};

type PendingCommit = {
  intent: AuditIntent;
  auditLog: AuditLog;
  expectedPartitionTail: PartitionTail;
  nextPartitionTail: PartitionTail;
};

type PipelineRuntime = {
  ready: boolean;
  flushing: boolean;
  logger: Logger | null;
  traceContext: TraceContext | null;
  nodeId: string;
  options: AuditPipelineOptions;
  flushTimer: ReturnType<typeof setInterval> | null;
  anchorTimer: ReturnType<typeof setInterval> | null;
  globalTail: GlobalTail;
  partitionTails: Map<number, PartitionTail>;
  backlogCount: number;
};

const runtime: PipelineRuntime = {
  ready: false,
  flushing: false,
  logger: null,
  traceContext: null,
  nodeId: 'core',
  options: DEFAULT_OPTIONS,
  flushTimer: null,
  anchorTimer: null,
  globalTail: {
    sequence: 0,
    hash: '',
  },
  partitionTails: new Map<number, PartitionTail>(),
  backlogCount: 0,
};

const ACTIVE_BACKLOG_STATUSES: readonly AuditIntentStatus[] = [
  'pending',
  'processing',
  'ready_for_global_commit',
  'failed_retriable',
];

const toSessionOption = (session: DbSession | undefined): { session?: ClientSession } =>
  session ? { session } : {};

const isDuplicateKeyError = (error: unknown): boolean => {
  if (typeof error !== 'object' || error === null) {
    return false;
  }
  const candidate = error as { code?: unknown };
  return candidate.code === 11000;
};

const isDuplicateKeyOnlyBulkError = (error: unknown): boolean => {
  if (typeof error !== 'object' || error === null) {
    return false;
  }

  const candidate = error as { writeErrors?: unknown };
  if (!Array.isArray(candidate.writeErrors) || candidate.writeErrors.length === 0) {
    return false;
  }

  return candidate.writeErrors.every((entry) => {
    if (typeof entry !== 'object' || entry === null) {
      return false;
    }
    const record = entry as { code?: unknown };
    return record.code === 11000;
  });
};

const AUDIT_LOG_WRITE_INCOMPLETE = 'AUDIT_LOG_WRITE_INCOMPLETE';
const AUDIT_LOG_WRITE_MISMATCH = 'AUDIT_LOG_WRITE_MISMATCH';
const AUDIT_GLOBAL_TAIL_CONFLICT = 'AUDIT_GLOBAL_TAIL_CONFLICT';
const AUDIT_PARTITION_TAIL_CONFLICT = 'AUDIT_PARTITION_TAIL_CONFLICT';

const getIntentsCollection = (db: Db): Collection<AuditIntent> =>
  db.collection<AuditIntent>(AUDIT_INTENTS_COLLECTION);

const getLogsCollection = (db: Db): Collection<AuditLog> =>
  db.collection<AuditLog>(AUDIT_COLLECTION);

const getStateCollection = (db: Db): Collection<AuditStateDocument> =>
  db.collection<AuditStateDocument>(AUDIT_STATE_COLLECTION);

const getPartitionStateCollection = (db: Db): Collection<AuditPartitionState> =>
  db.collection<AuditPartitionState>(AUDIT_PARTITION_STATE_COLLECTION);

const getAnchorCollection = (db: Db): Collection<AuditGlobalAnchor> =>
  db.collection<AuditGlobalAnchor>(AUDIT_GLOBAL_ANCHOR_COLLECTION);

const getFailureCollection = (db: Db): Collection<AuditFailureRecord> =>
  db.collection<AuditFailureRecord>(AUDIT_FAILURES_COLLECTION);

const loadGlobalTail = async (db: Db): Promise<GlobalTail> => {
  const latestLog = await getLogsCollection(db).findOne({}, { sort: { _sequence: -1 } });
  const sequence = latestLog?._sequence ?? 0;
  const hash = latestLog?._hash ?? '';
  await getStateCollection(db).findOneAndUpdate(
    { _id: AUDIT_STATE_ID },
    {
      $max: { value: sequence, global_last_sequence: sequence },
      $set: {
        global_last_hash: hash,
        updated_at: new Date(),
      },
      $setOnInsert: {
        _id: AUDIT_STATE_ID,
        backpressure_soft_limit: runtime.options.backlogSoftLimit,
        backpressure_hard_limit: runtime.options.backlogHardLimit,
      },
    },
    { upsert: true },
  );
  return { sequence, hash };
};

const loadPartitionTails = async (db: Db): Promise<Map<number, PartitionTail>> => {
  const tails = new Map<number, PartitionTail>();
  const rows = await getPartitionStateCollection(db).find({}).toArray();
  for (const row of rows) {
    tails.set(row.partition_id, {
      sequence: row.last_sequence,
      hash: row.last_hash,
    });
  }
  return tails;
};

const getOrLoadPartitionTail = async (
  db: Db,
  partitionId: number,
): Promise<PartitionTail> => {
  const existing = runtime.partitionTails.get(partitionId);
  if (existing) {
    return existing;
  }

  const id = `partition:${partitionId}`;
  const row = await getPartitionStateCollection(db).findOne({ _id: id });
  if (!row) {
    const initial: PartitionTail = { sequence: 0, hash: '' };
    runtime.partitionTails.set(partitionId, initial);
    return initial;
  }
  const loaded: PartitionTail = {
    sequence: row.last_sequence,
    hash: row.last_hash,
  };
  runtime.partitionTails.set(partitionId, loaded);
  return loaded;
};

const collectBacklogCount = async (
  db: Db,
  session: DbSession | undefined,
): Promise<number> => {
  const count = await getIntentsCollection(db).countDocuments(
    {
      status: { $in: ACTIVE_BACKLOG_STATUSES },
    },
    toSessionOption(session),
  );
  return count;
};

/**
 * 维护内存级 backlog 影子计数：
 * - 热路径优先读该计数，减少每次入队都走 countDocuments 的开销；
 * - 计数始终钳制为非负，避免异常回滚导致的负值污染。
 */
const adjustRuntimeBacklog = (delta: number): void => {
  runtime.backlogCount = Math.max(0, runtime.backlogCount + delta);
};

const isAuditCommitConflictError = (error: unknown): boolean =>
  error instanceof Error &&
  (error.message === AUDIT_GLOBAL_TAIL_CONFLICT ||
    error.message === AUDIT_PARTITION_TAIL_CONFLICT ||
    error.message === AUDIT_LOG_WRITE_INCOMPLETE ||
    error.message === AUDIT_LOG_WRITE_MISMATCH);

const toFailureRecord = (
  intent: AuditIntent,
  reason: string,
): AuditFailureRecord => ({
  event_id: intent.event_id,
  route_tag: intent.route_tag,
  reason,
  payload_digest: intent.payload_digest,
  payload_hmac: intent.payload_hmac,
  hmac_key_id: intent.hmac_key_id,
  created_at: new Date(),
});

const markIntentAsTerminalFailure = async (
  db: Db,
  intent: AuditIntent,
  reason: string,
  session: DbSession | undefined,
): Promise<void> => {
  const updateResult = await getIntentsCollection(db).updateOne(
    { event_id: intent.event_id },
    {
      $set: {
        status: 'failed_terminal',
        updated_at: new Date(),
        error_last: reason,
      },
      $inc: { attempt_count: 1 },
    },
    toSessionOption(session),
  );
  if (
    updateResult.modifiedCount > 0 &&
    intent.status !== 'failed_terminal' &&
    intent.status !== 'committed'
  ) {
    adjustRuntimeBacklog(-1);
  }
  await getFailureCollection(db).insertOne(
    toFailureRecord(intent, reason),
    toSessionOption(session),
  );
};

const markIntentForRetry = async (
  db: Db,
  intent: AuditIntent,
  reason: string,
): Promise<void> => {
  const collection = getIntentsCollection(db);
  const attempts = intent.attempt_count + 1;
  if (attempts >= runtime.options.maxRetryAttempts) {
    await markIntentAsTerminalFailure(db, intent, reason, undefined);
    return;
  }
  await collection.updateOne(
    { event_id: intent.event_id },
    {
      $set: {
        status: 'failed_retriable',
        updated_at: new Date(),
        error_last: reason,
        lease_owner: null,
        lease_until: null,
      },
      $inc: { attempt_count: 1 },
    },
  );
};

const buildCommitBatch = async (
  db: Db,
  intents: AuditIntent[],
  baseGlobalTail: GlobalTail,
): Promise<PendingCommit[]> => {
  const batch: PendingCommit[] = [];
  let globalSequence = baseGlobalTail.sequence;
  let globalHash = baseGlobalTail.hash;

  for (const intent of intents) {
    const digest = calculatePayloadDigest(intent.payload);
    const hmac = calculatePayloadHmac(digest, runtime.options.hmacSecret);
    if (digest !== intent.payload_digest || hmac !== intent.payload_hmac) {
      await markIntentAsTerminalFailure(db, intent, 'AUDIT_INTEGRITY_CHECK_FAILED', undefined);
      continue;
    }

    const partitionTail = await getOrLoadPartitionTail(db, intent.partition_id);
    const partitionSequence = partitionTail.sequence + 1;
    const partitionPreviousHash = partitionTail.hash;
    const partitionHash = calculatePartitionHash(
      intent.payload,
      partitionSequence,
      partitionPreviousHash,
    );

    globalSequence += 1;
    const auditLog: AuditLog = {
      ...intent.payload,
      event_id: intent.event_id,
      chain_version: 1,
      partition_id: intent.partition_id,
      partition_sequence: partitionSequence,
      partition_previous_hash: partitionPreviousHash,
      partition_hash: partitionHash,
      _sequence: globalSequence,
      _previous_hash: globalHash,
      _hash: '',
    };
    auditLog._hash = calculateHash(auditLog);
    globalHash = auditLog._hash;

    batch.push({
      intent,
      auditLog,
      expectedPartitionTail: {
        sequence: partitionTail.sequence,
        hash: partitionTail.hash,
      },
      nextPartitionTail: {
        sequence: partitionSequence,
        hash: partitionHash,
      },
    });
  }

  return batch;
};

const assertPersistedLogsForBatch = async (
  logs: Collection<AuditLog>,
  batch: PendingCommit[],
  session: DbSession | undefined,
): Promise<void> => {
  const eventIds = batch.map((item) => item.intent.event_id);
  const persisted = await logs.find(
    { event_id: { $in: eventIds } },
    toSessionOption(session),
  ).toArray();
  const persistedByEventId = new Map<string, AuditLog>();
  for (const row of persisted) {
    if (typeof row.event_id === 'string') {
      persistedByEventId.set(row.event_id, row);
    }
  }

  for (const item of batch) {
    const expected = item.auditLog;
    const actual = persistedByEventId.get(item.intent.event_id);
    if (!actual) {
      throw new Error(AUDIT_LOG_WRITE_INCOMPLETE);
    }

    if (
      actual._sequence !== expected._sequence ||
      actual._previous_hash !== expected._previous_hash ||
      actual._hash !== expected._hash ||
      actual.partition_id !== expected.partition_id ||
      actual.partition_sequence !== expected.partition_sequence ||
      actual.partition_hash !== expected.partition_hash ||
      actual.partition_previous_hash !== expected.partition_previous_hash
    ) {
      throw new Error(AUDIT_LOG_WRITE_MISMATCH);
    }
  }
};

const commitBatch = async (
  db: Db,
  batch: PendingCommit[],
  expectedGlobalTail: GlobalTail,
): Promise<void> => {
  if (batch.length === 0) {
    return;
  }

  const nextGlobalTail = {
    sequence: batch[batch.length - 1].auditLog._sequence,
    hash: batch[batch.length - 1].auditLog._hash,
  };
  const partitionTailUpdates = new Map<number, { expected: PartitionTail; next: PartitionTail }>();
  for (const item of batch) {
    const existing = partitionTailUpdates.get(item.intent.partition_id);
    if (!existing) {
      partitionTailUpdates.set(item.intent.partition_id, {
        expected: item.expectedPartitionTail,
        next: item.nextPartitionTail,
      });
      continue;
    }
    existing.next = item.nextPartitionTail;
  }

  await runInTransaction(db, async (session) => {
    const logs = getLogsCollection(db);
    const intents = getIntentsCollection(db);
    const partitions = getPartitionStateCollection(db);
    const state = getStateCollection(db);
    const now = new Date();

    /**
     * 逻辑块：批量写入日志链，显式容忍重复键。
     * 这是幂等保护的一部分：重试或重复消费时允许 audit_logs 已存在，但不能放大失败面。
     */
    if (typeof logs.bulkWrite === 'function') {
      try {
        await logs.bulkWrite(
          batch.map((item) => ({
            insertOne: { document: item.auditLog },
          })),
          { ...toSessionOption(session), ordered: false },
        );
      } catch (error) {
        if (!isDuplicateKeyError(error) && !isDuplicateKeyOnlyBulkError(error)) {
          throw error;
        }
        await assertPersistedLogsForBatch(logs, batch, session);
      }
    } else {
      /**
       * 逻辑块：兼容无 bulkWrite 的测试桩或受限驱动实现。
       * 在该分支保持旧语义逐条 insert，同时继续容忍重复键。
       */
      for (const item of batch) {
        try {
          await logs.insertOne(item.auditLog, toSessionOption(session));
        } catch (error) {
          if (!isDuplicateKeyError(error)) {
            throw error;
          }
        }
      }
      await assertPersistedLogsForBatch(logs, batch, session);
    }

    /**
     * 逻辑块：intent 状态批量推进为 committed。
     * 使用 $unset 清理 error_last，避免旧错误信息在成功提交后残留。
     */
    if (typeof intents.bulkWrite === 'function') {
      await intents.bulkWrite(
        batch.map((item) => ({
          updateOne: {
            filter: { event_id: item.intent.event_id },
            update: {
              $set: {
                status: 'committed',
                committed_at: now,
                global_sequence: item.auditLog._sequence,
                updated_at: now,
                lease_owner: null,
                lease_until: null,
              },
              $unset: {
                error_last: '',
              },
            },
          },
        })),
        { ...toSessionOption(session), ordered: false },
      );
    } else {
      for (const item of batch) {
        await intents.updateOne(
          { event_id: item.intent.event_id },
          {
            $set: {
              status: 'committed',
              committed_at: now,
              global_sequence: item.auditLog._sequence,
              updated_at: now,
              lease_owner: null,
              lease_until: null,
            },
            $unset: {
              error_last: '',
            },
          },
          toSessionOption(session),
        );
      }
    }

    /**
     * 逻辑块：按分区聚合尾指针后再执行 CAS upsert。
     * 过滤条件绑定 expected tail，只有“我看到的旧值”仍成立时才允许推进分区链；
     * 若并发写者已推进尾指针，会触发冲突并整体回滚，避免分区链被覆写。
     */
    if (typeof partitions.bulkWrite === 'function') {
      try {
        await partitions.bulkWrite(
          [...partitionTailUpdates.entries()].map(([partitionId, guard]) => ({
            updateOne: {
              filter: {
                _id: `partition:${partitionId}`,
                partition_id: partitionId,
                last_sequence: guard.expected.sequence,
                last_hash: guard.expected.hash,
              },
              update: {
                $set: {
                  partition_id: partitionId,
                  last_sequence: guard.next.sequence,
                  last_hash: guard.next.hash,
                  updated_at: now,
                },
              },
              upsert: true,
            },
          })),
          { ...toSessionOption(session), ordered: false },
        );
      } catch (error) {
        if (isDuplicateKeyError(error) || isDuplicateKeyOnlyBulkError(error)) {
          throw new Error(AUDIT_PARTITION_TAIL_CONFLICT);
        }
        throw error;
      }
    } else {
      for (const [partitionId, guard] of partitionTailUpdates.entries()) {
        try {
          await partitions.updateOne(
            {
              _id: `partition:${partitionId}`,
              partition_id: partitionId,
              last_sequence: guard.expected.sequence,
              last_hash: guard.expected.hash,
            },
            {
              $set: {
                partition_id: partitionId,
                last_sequence: guard.next.sequence,
                last_hash: guard.next.hash,
                updated_at: now,
              },
            },
            { ...toSessionOption(session), upsert: true },
          );
        } catch (error) {
          if (isDuplicateKeyError(error)) {
            throw new Error(AUDIT_PARTITION_TAIL_CONFLICT);
          }
          throw error;
        }
      }
    }

    /**
     * 逻辑块：全局 tail 更新采用 CAS（expected sequence/hash）。
     * 这一步是多写安全的最终闸门：只有持有正确前驱的批次才能提交到全局链头。
     */
    const stateUpdate = await state.updateOne(
      {
        _id: AUDIT_STATE_ID,
        global_last_sequence: expectedGlobalTail.sequence,
        global_last_hash: expectedGlobalTail.hash,
      },
      {
        $set: {
          value: nextGlobalTail.sequence,
          global_last_sequence: nextGlobalTail.sequence,
          global_last_hash: nextGlobalTail.hash,
          updated_at: now,
        },
        $setOnInsert: {
          _id: AUDIT_STATE_ID,
        },
      },
      { ...toSessionOption(session), upsert: false },
    );
    if (stateUpdate.modifiedCount === 0) {
      throw new Error(AUDIT_GLOBAL_TAIL_CONFLICT);
    }
  });

  runtime.globalTail = nextGlobalTail;
  for (const [partitionId, guard] of partitionTailUpdates.entries()) {
    runtime.partitionTails.set(partitionId, guard.next);
  }
  adjustRuntimeBacklog(-batch.length);
};

const claimPendingIntents = async (
  db: Db,
): Promise<AuditIntent[]> => {
  const collection = getIntentsCollection(db);
  const now = new Date();
  const leaseUntil = new Date(now.getTime() + runtime.options.leaseDurationMs);

  const rows = await collection
    .find({ status: { $in: ['pending', 'failed_retriable'] } })
    .sort({ created_at: 1, event_id: 1 })
    .limit(runtime.options.batchSize)
    .toArray();

  /**
   * 第一优先级消费 pending/failed_retriable；
   * 若批次未满，再回收 lease 过期的 processing，防止进程崩溃后出现“永久卡住”的积压。
   */
  let claimCandidates = rows;
  if (claimCandidates.length < runtime.options.batchSize) {
    /**
     * 逻辑块：只回收“可判定已过期”的 processing，避免全量拉取 processing 后在 JS 侧过滤。
     * 这样可以显著降低高并发下 claim 阶段的 CPU 与 Mongo 往返开销。
     */
    const processing = await collection
      .find({
        status: 'processing',
        $or: [
          { lease_until: { $lte: now } },
          { lease_until: null },
          { lease_until: { $exists: false } },
        ],
      })
      .sort({ created_at: 1, event_id: 1 })
      .limit(runtime.options.batchSize - claimCandidates.length)
      .toArray();
    claimCandidates = [
      ...claimCandidates,
      ...processing,
    ];
  }

  const claimed: AuditIntent[] = [];
  for (const row of claimCandidates) {
    const filter: Record<string, unknown> = {
      event_id: row.event_id,
      status: row.status,
    };
    if (row.status === 'processing') {
      filter.lease_owner = row.lease_owner;
      filter.lease_until = row.lease_until;
    }
    const updateResult = await collection.updateOne(
      filter,
      {
        $set: {
          status: 'processing',
          updated_at: now,
          lease_owner: runtime.nodeId,
          lease_until: leaseUntil,
        },
      },
    );
    if (updateResult.modifiedCount > 0) {
      claimed.push({
        ...row,
        status: 'processing',
        updated_at: now,
        lease_owner: runtime.nodeId,
        lease_until: leaseUntil,
      });
    }
  }
  return claimed;
};

const drainPendingIntents = async (
  db: Db,
  traceContext: TraceContext,
): Promise<void> => {
  if (runtime.flushing) {
    return;
  }
  runtime.flushing = true;
  const logger = runtime.logger ?? createLogger(traceContext);
  runtime.logger = logger;
  try {
    const claimed = await claimPendingIntents(db);
    if (claimed.length === 0) {
      return;
    }

    const baseGlobalTail: GlobalTail = {
      sequence: runtime.globalTail.sequence,
      hash: runtime.globalTail.hash,
    };
    const batch = await buildCommitBatch(db, claimed, baseGlobalTail);
    await commitBatch(db, batch, baseGlobalTail);
  } catch (error) {
    const isConflict = isAuditCommitConflictError(error);
    logger.error('[AuditPipeline] flush failed', {
      error: error instanceof Error ? error.message : String(error),
    });
    if (isConflict) {
      /**
       * 逻辑块：多写竞争导致 tail 冲突时，先回源刷新影子 tail，再释放本轮 lease。
       * 这类冲突属于并发竞争而非数据损坏，不应累计 attempt_count，避免误判为 terminal failure。
       */
      runtime.globalTail = await loadGlobalTail(db);
      runtime.partitionTails = await loadPartitionTails(db);
      runtime.backlogCount = await collectBacklogCount(db, undefined);
    }
    const claimed = await getIntentsCollection(db)
      .find({
        status: 'processing',
        lease_owner: runtime.nodeId,
      })
      .sort({ created_at: 1, event_id: 1 })
      .limit(runtime.options.batchSize)
      .toArray();
    for (const intent of claimed) {
      const message = error instanceof Error ? error.message : String(error);
      if (isConflict) {
        await getIntentsCollection(db).updateOne(
          { event_id: intent.event_id },
          {
            $set: {
              status: 'pending',
              updated_at: new Date(),
              error_last: message,
              lease_owner: null,
              lease_until: null,
            },
          },
        );
      } else {
        await markIntentForRetry(db, intent, message);
      }
    }
  } finally {
    runtime.flushing = false;
  }
};

const writeAnchor = async (
  db: Db,
): Promise<void> => {
  if (!runtime.ready) {
    return;
  }
  const heads: AuditGlobalAnchorHead[] = [...runtime.partitionTails.entries()]
    .map(([partitionId, tail]) => ({
      partition_id: partitionId,
      last_sequence: tail.sequence,
      last_hash: tail.hash,
    }))
    .sort((left, right) => left.partition_id - right.partition_id);

  if (heads.length === 0) {
    return;
  }

  const latestAnchor = await getAnchorCollection(db).findOne({}, { sort: { ts: -1 } });
  const previousAnchorHash = latestAnchor?.anchor_hash ?? '';
  const payload = {
    partition_heads: heads,
    previous_anchor_hash: previousAnchorHash,
  };
  const anchorHash = createHash('sha256')
    .update(stableStringifyUnknown(payload))
    .digest('hex');
  await getAnchorCollection(db).insertOne({
    anchor_id: randomUUID(),
    ts: Date.now(),
    partition_heads: heads,
    previous_anchor_hash: previousAnchorHash,
    anchor_hash: anchorHash,
  });
};

export const isAuditPipelineReady = (): boolean => runtime.ready;

export const startAuditPipeline = async (
  db: Db,
  traceContext: TraceContext,
  options: AuditPipelineStartOptions = {},
): Promise<void> => {
  if (runtime.ready) {
    return;
  }
  runtime.options = normalizeOptions(options);
  runtime.traceContext = traceContext;
  runtime.nodeId = traceContext.nodeId;
  runtime.logger = createLogger(traceContext);
  runtime.globalTail = await loadGlobalTail(db);
  runtime.partitionTails = await loadPartitionTails(db);
  runtime.backlogCount = await collectBacklogCount(db, undefined);

  if (runtime.options.enableBackgroundLoops) {
    runtime.flushTimer = setInterval(() => {
      void drainPendingIntents(db, traceContext);
    }, runtime.options.flushIntervalMs);
    runtime.anchorTimer = setInterval(() => {
      void writeAnchor(db);
    }, runtime.options.anchorIntervalMs);
  }

  runtime.ready = true;
  const logger = runtime.logger ?? createLogger(traceContext);
  runtime.logger = logger;
  logger.info('[AuditPipeline] started', {
    partition_count: runtime.options.partitionCount,
    batch_size: runtime.options.batchSize,
    flush_interval_ms: runtime.options.flushIntervalMs,
    backlog_hard_limit: runtime.options.backlogHardLimit,
    backlog_count: runtime.backlogCount,
  });
};

export const stopAuditPipeline = async (): Promise<void> => {
  if (runtime.flushTimer) {
    clearInterval(runtime.flushTimer);
  }
  if (runtime.anchorTimer) {
    clearInterval(runtime.anchorTimer);
  }
  runtime.flushTimer = null;
  runtime.anchorTimer = null;
  runtime.ready = false;
  runtime.flushing = false;
  runtime.logger = null;
  runtime.traceContext = null;
  runtime.globalTail = { sequence: 0, hash: '' };
  runtime.partitionTails = new Map<number, PartitionTail>();
  runtime.backlogCount = 0;
};

export const enqueueAuditIntent = async (
  db: Db,
  event: AuditEventInput,
  options: EnqueueOptions,
): Promise<IntakeResult> => {
  if (!runtime.ready) {
    return {
      accepted: false,
      reason: 'pipeline_unavailable',
    };
  }

  /**
   * 背压判定采用“两段式”：
   * - 先看内存影子计数（O(1)）；
   * - 仅在触线时回源 DB 校准，避免计数漂移导致误判。
   */
  if (runtime.backlogCount >= runtime.options.backlogHardLimit) {
    const backlog = await collectBacklogCount(db, options.session);
    runtime.backlogCount = backlog;
    if (backlog >= runtime.options.backlogHardLimit) {
      return {
        accepted: false,
        reason: 'backpressure',
        retryAfterSeconds: 1,
      };
    }
  }

  const digest = calculatePayloadDigest(event);
  const hmac = calculatePayloadHmac(digest, runtime.options.hmacSecret);
  const now = new Date();
  const eventId = randomUUID();

  const intent: AuditIntent = {
    event_id: eventId,
    route_tag: options.routeTag,
    partition_id: calculatePartitionId(event, runtime.options.partitionCount),
    status: 'pending',
    lease_owner: null,
    lease_until: null,
    attempt_count: 0,
    created_at: now,
    updated_at: now,
    payload: event,
    payload_digest: digest,
    payload_hmac: hmac,
    hmac_key_id: runtime.options.hmacKeyId,
  };

  let inserted = false;
  try {
    await getIntentsCollection(db).insertOne(intent, toSessionOption(options.session));
    inserted = true;
  } catch (error) {
    if (!isDuplicateKeyError(error)) {
      throw error;
    }
  }
  if (inserted) {
    adjustRuntimeBacklog(1);
  }
  return {
    accepted: true,
  };
};

export const recordAuditEvent = async (
  db: Db,
  event: AuditEventInput,
  options: RecordAuditOptions,
): Promise<AuditLog | null> => {
  if (!runtime.ready) {
    return logAuditEvent(db, event);
  }
  const intake = await enqueueAuditIntent(db, event, {
    routeTag: options.routeTag,
    session: options.session,
  });
  if (!intake.accepted && intake.reason === 'backpressure') {
    throw new DomainError('AUDIT_BACKPRESSURE', {
      meta: {
        retry_after_seconds: intake.retryAfterSeconds ?? 1,
      },
    });
  }
  if (!intake.accepted) {
    return logAuditEvent(db, event);
  }
  return null;
};

export const drainAuditPipelineOnce = async (
  db: Db,
  traceContext: TraceContext,
): Promise<void> => {
  if (!runtime.ready) {
    return;
  }
  await drainPendingIntents(db, traceContext);
};

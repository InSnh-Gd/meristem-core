import { createHash, createHmac, randomUUID } from 'crypto';
import type { ClientSession, Collection, Db } from 'mongodb';
import { DomainError } from '../errors/domain-error';
import type { DbSession } from '../db/transactions';
import { runInTransaction } from '../db/transactions';
import { createLogger } from '../utils/logger';
import type { TraceContext } from '../utils/trace-context';
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

type AuditPipelineStartOptions = {
  enableBackgroundLoops?: boolean;
  partitionCount?: number;
  batchSize?: number;
  flushIntervalMs?: number;
  anchorIntervalMs?: number;
  backlogSoftLimit?: number;
  backlogHardLimit?: number;
  leaseDurationMs?: number;
  maxRetryAttempts?: number;
  hmacSecret?: string;
  hmacKeyId?: string;
};

type AuditPipelineOptions = {
  enableBackgroundLoops: boolean;
  partitionCount: number;
  batchSize: number;
  flushIntervalMs: number;
  anchorIntervalMs: number;
  backlogSoftLimit: number;
  backlogHardLimit: number;
  leaseDurationMs: number;
  maxRetryAttempts: number;
  hmacSecret: string;
  hmacKeyId: string;
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
  nextPartitionTail: PartitionTail;
};

type PipelineRuntime = {
  ready: boolean;
  flushing: boolean;
  traceContext: TraceContext | null;
  nodeId: string;
  options: AuditPipelineOptions;
  flushTimer: ReturnType<typeof setInterval> | null;
  anchorTimer: ReturnType<typeof setInterval> | null;
  globalTail: GlobalTail;
  partitionTails: Map<number, PartitionTail>;
  backlogCount: number;
};

const DEFAULT_OPTIONS: AuditPipelineOptions = {
  enableBackgroundLoops: true,
  partitionCount: 16,
  batchSize: 32,
  flushIntervalMs: 20,
  anchorIntervalMs: 1_000,
  backlogSoftLimit: 3_000,
  backlogHardLimit: 8_000,
  leaseDurationMs: 10_000,
  maxRetryAttempts: 5,
  hmacSecret: process.env.MERISTEM_AUDIT_HMAC_SECRET ?? 'meristem-audit-default-secret',
  hmacKeyId: process.env.MERISTEM_AUDIT_HMAC_KEY_ID ?? 'audit-hmac-v1',
};

const runtime: PipelineRuntime = {
  ready: false,
  flushing: false,
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

const toPositiveInteger = (value: number, fallback: number): number =>
  Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;

const normalizeOptions = (
  options: AuditPipelineStartOptions = {},
): AuditPipelineOptions => ({
  enableBackgroundLoops: options.enableBackgroundLoops ?? DEFAULT_OPTIONS.enableBackgroundLoops,
  partitionCount: toPositiveInteger(options.partitionCount ?? DEFAULT_OPTIONS.partitionCount, DEFAULT_OPTIONS.partitionCount),
  batchSize: toPositiveInteger(options.batchSize ?? DEFAULT_OPTIONS.batchSize, DEFAULT_OPTIONS.batchSize),
  flushIntervalMs: toPositiveInteger(options.flushIntervalMs ?? DEFAULT_OPTIONS.flushIntervalMs, DEFAULT_OPTIONS.flushIntervalMs),
  anchorIntervalMs: toPositiveInteger(options.anchorIntervalMs ?? DEFAULT_OPTIONS.anchorIntervalMs, DEFAULT_OPTIONS.anchorIntervalMs),
  backlogSoftLimit: toPositiveInteger(options.backlogSoftLimit ?? DEFAULT_OPTIONS.backlogSoftLimit, DEFAULT_OPTIONS.backlogSoftLimit),
  backlogHardLimit: toPositiveInteger(options.backlogHardLimit ?? DEFAULT_OPTIONS.backlogHardLimit, DEFAULT_OPTIONS.backlogHardLimit),
  leaseDurationMs: toPositiveInteger(options.leaseDurationMs ?? DEFAULT_OPTIONS.leaseDurationMs, DEFAULT_OPTIONS.leaseDurationMs),
  maxRetryAttempts: toPositiveInteger(options.maxRetryAttempts ?? DEFAULT_OPTIONS.maxRetryAttempts, DEFAULT_OPTIONS.maxRetryAttempts),
  hmacSecret: options.hmacSecret ?? DEFAULT_OPTIONS.hmacSecret,
  hmacKeyId: options.hmacKeyId ?? DEFAULT_OPTIONS.hmacKeyId,
});

const isDuplicateKeyError = (error: unknown): boolean => {
  if (typeof error !== 'object' || error === null) {
    return false;
  }
  const candidate = error as { code?: unknown };
  return candidate.code === 11000;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const canonicalizeValue = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map((item) => canonicalizeValue(item));
  }
  if (isRecord(value)) {
    const normalized: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      const candidate = value[key];
      if (candidate !== undefined) {
        normalized[key] = canonicalizeValue(candidate);
      }
    }
    return normalized;
  }
  return value;
};

const stableStringifyUnknown = (value: unknown): string =>
  JSON.stringify(canonicalizeValue(value));

const calculatePayloadDigest = (payload: AuditEventInput): string =>
  createHash('sha256').update(stableStringifyUnknown(payload)).digest('hex');

const calculatePayloadHmac = (
  digest: string,
  secret: string,
): string => createHmac('sha256', secret).update(digest).digest('hex');

const calculatePartitionHash = (
  payload: AuditEventInput,
  partitionSequence: number,
  partitionPreviousHash: string,
): string => {
  const input = {
    ...payload,
    partition_sequence: partitionSequence,
    partition_previous_hash: partitionPreviousHash,
  };
  return createHash('sha256').update(stableStringifyUnknown(input)).digest('hex');
};

const calculatePartitionId = (
  payload: AuditEventInput,
  partitionCount: number,
): number => {
  const seed = `${payload.node_id}|${payload.trace_id}|${payload.source}`;
  const digest = createHash('sha256').update(seed).digest();
  return digest.readUInt32BE(0) % partitionCount;
};

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
): Promise<PendingCommit[]> => {
  const batch: PendingCommit[] = [];
  let globalSequence = runtime.globalTail.sequence;
  let globalHash = runtime.globalTail.hash;

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
      nextPartitionTail: {
        sequence: partitionSequence,
        hash: partitionHash,
      },
    });
  }

  return batch;
};

const commitBatch = async (
  db: Db,
  batch: PendingCommit[],
): Promise<void> => {
  if (batch.length === 0) {
    return;
  }

  const nextGlobalTail = {
    sequence: batch[batch.length - 1].auditLog._sequence,
    hash: batch[batch.length - 1].auditLog._hash,
  };

  await runInTransaction(db, async (session) => {
    const logs = getLogsCollection(db);
    const intents = getIntentsCollection(db);
    const partitions = getPartitionStateCollection(db);
    const state = getStateCollection(db);

    for (const item of batch) {
      try {
        await logs.insertOne(item.auditLog, toSessionOption(session));
      } catch (error) {
        if (!isDuplicateKeyError(error)) {
          throw error;
        }
      }
      await intents.updateOne(
        { event_id: item.intent.event_id },
        {
          $set: {
            status: 'committed',
            committed_at: new Date(),
            global_sequence: item.auditLog._sequence,
            updated_at: new Date(),
            lease_owner: null,
            lease_until: null,
            error_last: undefined,
          },
        },
        toSessionOption(session),
      );
      await partitions.updateOne(
        { _id: `partition:${item.intent.partition_id}` },
        {
          $set: {
            partition_id: item.intent.partition_id,
            last_sequence: item.nextPartitionTail.sequence,
            last_hash: item.nextPartitionTail.hash,
            updated_at: new Date(),
          },
        },
        { ...toSessionOption(session), upsert: true },
      );
    }

    await state.updateOne(
      { _id: AUDIT_STATE_ID },
      {
        $set: {
          value: nextGlobalTail.sequence,
          global_last_sequence: nextGlobalTail.sequence,
          global_last_hash: nextGlobalTail.hash,
          updated_at: new Date(),
        },
        $setOnInsert: {
          _id: AUDIT_STATE_ID,
        },
      },
      { ...toSessionOption(session), upsert: true },
    );
  });

  runtime.globalTail = nextGlobalTail;
  for (const item of batch) {
    runtime.partitionTails.set(item.intent.partition_id, item.nextPartitionTail);
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
    const processing = await collection
      .find({ status: 'processing' })
      .sort({ created_at: 1, event_id: 1 })
      .toArray();
    const expired = processing.filter((intent) => {
      if (!(intent.lease_until instanceof Date)) {
        return true;
      }
      return intent.lease_until.getTime() <= now.getTime();
    });
    claimCandidates = [
      ...claimCandidates,
      ...expired.slice(0, runtime.options.batchSize - claimCandidates.length),
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
  const logger = createLogger(traceContext);
  try {
    const claimed = await claimPendingIntents(db);
    if (claimed.length === 0) {
      return;
    }

    const batch = await buildCommitBatch(db, claimed);
    await commitBatch(db, batch);
  } catch (error) {
    logger.error('[AuditPipeline] flush failed', {
      error: error instanceof Error ? error.message : String(error),
    });
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
      await markIntentForRetry(db, intent, message);
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
  const logger = createLogger(traceContext);
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

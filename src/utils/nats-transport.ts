import type { NatsConnection } from 'nats';
import { getNats } from '../nats/connection';
import type { LogEnvelope, LogLevel } from '@insnh-gd/meristem-shared';
import {
  appendEntry,
  createEntry,
  createRingBuffer,
  prependBatch,
  takeBatch,
  type BufferedEntry,
} from './nats-transport-buffer';

const DEFAULT_MAX_BUFFER_BYTES = 5 * 1024 * 1024;
const DEFAULT_MIN_BATCH_SIZE = 50;
const DEFAULT_MAX_BATCH_SIZE = 100;
const DEFAULT_FLUSH_INTERVAL_MS = 100;
const DEFAULT_MAX_PAYLOAD_BYTES = 4 * 1024;
const DEFAULT_ENABLE_FRAGMENTATION = true;
const DEFAULT_MAX_FRAGMENT_COUNT = 16;
const DEFAULT_FRAGMENT_TTL_MS = 5_000;
const FRAGMENT_SCHEMA_VERSION = 1;

const SYSTEM_TOPIC_PREFIX = 'meristem.v1.logs.sys.';
const TASK_TOPIC_PREFIX = 'meristem.v1.logs.task.';

export type { LogEnvelope };

type NatsConnectionLike = Pick<NatsConnection, 'publish' | 'jetstreamManager'>;

export type NatsTransportOptions = Readonly<{
  readonly bufferMaxBytes?: number;
  readonly minBatchSize?: number;
  readonly maxBatchSize?: number;
  readonly flushIntervalMs?: number;
  readonly maxPayloadBytes?: number;
  readonly enableFragmentation?: boolean;
  readonly maxFragmentCount?: number;
  readonly fragmentTtlMs?: number;
  readonly getConnection?: () => Promise<NatsConnectionLike>;
  readonly encode?: (value: string) => Uint8Array;
}>;

export type TransportStats = Readonly<{
  readonly bufferedCount: number;
  readonly bufferedBytes: number;
  readonly droppedCount: number;
  readonly fragmentedCount: number;
  readonly oversizeDropCount: number;
  readonly jetStreamAvailable: boolean | null;
}>;

export type NatsTransport = Readonly<{
  readonly write: (input: unknown) => void;
  readonly flush: (allowPartial?: boolean) => Promise<void>;
  readonly stop: () => Promise<void>;
  readonly stats: () => TransportStats;
}>;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isLogLevel = (value: unknown): value is LogLevel =>
  value === 'DEBUG' ||
  value === 'INFO' ||
  value === 'WARN' ||
  value === 'ERROR' ||
  value === 'FATAL';

const isLogEnvelope = (value: unknown): value is LogEnvelope => {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.ts === 'number' &&
    isLogLevel(value.level) &&
    typeof value.node_id === 'string' &&
    typeof value.source === 'string' &&
    typeof value.trace_id === 'string' &&
    typeof value.content === 'string' &&
    isRecord(value.meta)
  );
};

const parseJson = (value: string): unknown => {
  try {
    return JSON.parse(value);
  } catch {
    return undefined;
  }
};

const parseEnvelope = (input: unknown): LogEnvelope | null => {
  if (isLogEnvelope(input)) {
    return input;
  }

  if (typeof input === 'string') {
    const parsed = parseJson(input);
    return isLogEnvelope(parsed) ? parsed : null;
  }

  return null;
};

const safeStringify = (value: unknown): string | null => {
  try {
    return JSON.stringify(value);
  } catch {
    return null;
  }
};

const extractTaskId = (meta: Record<string, unknown>): string | undefined => {
  const direct = meta.taskId;
  if (typeof direct === 'string' && direct.length > 0) {
    return direct;
  }

  const snake = meta.task_id;
  if (typeof snake === 'string' && snake.length > 0) {
    return snake;
  }

  return undefined;
};

const resolveSubject = (envelope: LogEnvelope): string => {
  const taskId = extractTaskId(envelope.meta);
  if (taskId) {
    return `${TASK_TOPIC_PREFIX}${envelope.node_id}.${taskId}`;
  }
  return `${SYSTEM_TOPIC_PREFIX}${envelope.node_id}`;
};

const clampBatchSize = (value: number, fallback: number): number => {
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return value;
};

const clampPositiveInt = (value: number | undefined, fallback: number): number => {
  if (!Number.isFinite(value) || value === undefined) {
    return fallback;
  }
  if (value <= 0) {
    return fallback;
  }
  return Math.floor(value);
};

type TransportFragmentEnvelope = Readonly<{
  __meristem_fragment_v: 1;
  fragment_id: string;
  fragment_index: number;
  fragment_total: number;
  fragment_subject: string;
  fragment_expires_at: number;
  trace_id: string;
  payload_chunk: string;
}>;

const createFragmentId = (traceId: string): string => `${traceId}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

const encodeFragmentPayload = (input: {
  fragmentId: string;
  fragmentIndex: number;
  fragmentTotal: number;
  fragmentSubject: string;
  fragmentExpiresAt: number;
  traceId: string;
  payloadChunk: string;
  encode: (value: string) => Uint8Array;
}): Uint8Array | null => {
  const fragment: TransportFragmentEnvelope = {
    __meristem_fragment_v: FRAGMENT_SCHEMA_VERSION,
    fragment_id: input.fragmentId,
    fragment_index: input.fragmentIndex,
    fragment_total: input.fragmentTotal,
    fragment_subject: input.fragmentSubject,
    fragment_expires_at: input.fragmentExpiresAt,
    trace_id: input.traceId,
    payload_chunk: input.payloadChunk,
  };
  const serialized = safeStringify(fragment);
  if (!serialized) {
    return null;
  }
  return input.encode(serialized);
};

/**
 * 逻辑块：控制面大包采用“保语义分片”策略。
 * - 目标：单条 payload 超预算时，尽可能保持原 subject 与 trace 语义可追踪。
 * - 原因：不同网络路径 MTU 波动会导致控制面大包不稳定。
 * - 降级：若分片数量超限或单分片仍超限，直接拒绝并计入 oversizeDropCount。
 */
const buildFragmentEntries = (input: {
  subject: string;
  traceId: string;
  serializedPayload: string;
  maxPayloadBytes: number;
  maxFragmentCount: number;
  fragmentTtlMs: number;
  encode: (value: string) => Uint8Array;
}): readonly BufferedEntry[] | null => {
  const fragmentId = createFragmentId(input.traceId);
  const fragmentExpiresAt = Date.now() + input.fragmentTtlMs;
  const chunksQueue: string[] = [input.serializedPayload];
  const chunks: string[] = [];

  while (chunksQueue.length > 0) {
    const chunk = chunksQueue.shift();
    if (chunk === undefined) {
      continue;
    }

    const encodedChunk = encodeFragmentPayload({
      fragmentId,
      fragmentIndex: 0,
      fragmentTotal: input.maxFragmentCount,
      fragmentSubject: input.subject,
      fragmentExpiresAt,
      traceId: input.traceId,
      payloadChunk: chunk,
      encode: input.encode,
    });

    if (!encodedChunk) {
      return null;
    }

    if (encodedChunk.byteLength <= input.maxPayloadBytes) {
      chunks.push(chunk);
      if (chunks.length > input.maxFragmentCount) {
        return null;
      }
      continue;
    }

    if (chunk.length <= 1) {
      return null;
    }

    const splitAt = Math.floor(chunk.length / 2);
    const left = chunk.slice(0, splitAt);
    const right = chunk.slice(splitAt);
    chunksQueue.unshift(right);
    chunksQueue.unshift(left);

    if (chunksQueue.length + chunks.length > input.maxFragmentCount * 2) {
      return null;
    }
  }

  if (chunks.length === 0 || chunks.length > input.maxFragmentCount) {
    return null;
  }

  const entries: BufferedEntry[] = [];
  const fragmentTotal = chunks.length;
  for (let index = 0; index < chunks.length; index += 1) {
    const encodedPayload = encodeFragmentPayload({
      fragmentId,
      fragmentIndex: index,
      fragmentTotal,
      fragmentSubject: input.subject,
      fragmentExpiresAt,
      traceId: input.traceId,
      payloadChunk: chunks[index],
      encode: input.encode,
    });

    if (!encodedPayload) {
      return null;
    }

    if (encodedPayload.byteLength > input.maxPayloadBytes) {
      return null;
    }

    entries.push(createEntry(input.subject, encodedPayload));
  }

  return Object.freeze(entries);
};

export function createNatsTransport(options: NatsTransportOptions = {}): NatsTransport {
  const bufferMaxBytes = options.bufferMaxBytes ?? DEFAULT_MAX_BUFFER_BYTES;
  const minBatchSize = clampBatchSize(options.minBatchSize ?? DEFAULT_MIN_BATCH_SIZE, DEFAULT_MIN_BATCH_SIZE);
  const resolvedMaxBatch = clampBatchSize(options.maxBatchSize ?? DEFAULT_MAX_BATCH_SIZE, DEFAULT_MAX_BATCH_SIZE);
  const maxBatchSize = Math.max(minBatchSize, resolvedMaxBatch);
  const flushIntervalMs = options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
  const maxPayloadBytes = clampPositiveInt(options.maxPayloadBytes, DEFAULT_MAX_PAYLOAD_BYTES);
  const enableFragmentation = options.enableFragmentation ?? DEFAULT_ENABLE_FRAGMENTATION;
  const maxFragmentCount = clampPositiveInt(options.maxFragmentCount, DEFAULT_MAX_FRAGMENT_COUNT);
  const fragmentTtlMs = clampPositiveInt(options.fragmentTtlMs, DEFAULT_FRAGMENT_TTL_MS);
  const getConnection = options.getConnection ?? getNats;
  const textEncoder = new TextEncoder();
  const encoder = options.encode ?? ((value: string) => textEncoder.encode(value));

  let bufferState = createRingBuffer(bufferMaxBytes);
  let droppedCount = 0;
  let fragmentedCount = 0;
  let oversizeDropCount = 0;
  let jetStreamAvailable: boolean | null = null;
  let flushTimer: ReturnType<typeof setTimeout> | null = null;
  let flushPromise: Promise<void> | null = null;

  const scheduleFlush = (): void => {
    if (flushTimer) {
      return;
    }

    flushTimer = setTimeout(() => {
      void flush(true);
    }, flushIntervalMs);
  };

  const clearFlushTimer = (): void => {
    if (!flushTimer) {
      return;
    }
    clearTimeout(flushTimer);
    flushTimer = null;
  };

  const recordDrop = (count: number): void => {
    if (count > 0) {
      droppedCount += count;
    }
  };

  const updateJetStreamAvailability = async (connection: NatsConnectionLike): Promise<void> => {
    try {
      await connection.jetstreamManager();
      jetStreamAvailable = true;
    } catch {
      jetStreamAvailable = false;
    }
  };

  const publishBatch = async (connection: NatsConnectionLike, batch: readonly BufferedEntry[]): Promise<readonly BufferedEntry[]> => {
    for (let index = 0; index < batch.length; index += 1) {
      const entry = batch[index];
      try {
        connection.publish(entry.subject, entry.payload);
      } catch {
        return batch.slice(index);
      }
    }

    return [] as readonly BufferedEntry[];
  };

  const runFlush = async (allowPartial: boolean): Promise<void> => {
    clearFlushTimer();

    if (bufferState.entries.length === 0) {
      return;
    }

    let connection: NatsConnectionLike;

    try {
      connection = await getConnection();
    } catch {
      scheduleFlush();
      return;
    }

    await updateJetStreamAvailability(connection);

    while (bufferState.entries.length > 0) {
      const available = bufferState.entries.length;
      const batchSize = Math.min(maxBatchSize, available);
      if (!allowPartial && batchSize < minBatchSize) {
        break;
      }

      const { batch, state } = takeBatch(bufferState, batchSize);
      bufferState = state;

      const remaining = await publishBatch(connection, batch);
      if (remaining.length > 0) {
        bufferState = prependBatch(bufferState, remaining);
        break;
      }
    }

    if (bufferState.entries.length > 0) {
      scheduleFlush();
    }
  };

  const flush = async (allowPartial: boolean = true): Promise<void> => {
    if (flushPromise) {
      return flushPromise;
    }

    flushPromise = runFlush(allowPartial).finally(() => {
      flushPromise = null;
    });

    return flushPromise;
  };

  const write = (input: unknown): void => {
    const envelope = parseEnvelope(input);
    if (!envelope) {
      droppedCount += 1;
      return;
    }

    const serialized = safeStringify(envelope);
    if (!serialized) {
      droppedCount += 1;
      return;
    }

    const subject = resolveSubject(envelope);
    const payload = encoder(serialized);

    if (payload.byteLength <= maxPayloadBytes) {
      const entry = createEntry(subject, payload);
      const result = appendEntry(bufferState, entry);
      bufferState = result.state;
      recordDrop(result.dropped);
    } else if (enableFragmentation) {
      const fragments = buildFragmentEntries({
        subject,
        traceId: envelope.trace_id,
        serializedPayload: serialized,
        maxPayloadBytes,
        maxFragmentCount,
        fragmentTtlMs,
        encode: encoder,
      });

      if (!fragments) {
        droppedCount += 1;
        oversizeDropCount += 1;
        return;
      }

      fragmentedCount += fragments.length;
      for (const fragmentEntry of fragments) {
        const result = appendEntry(bufferState, fragmentEntry);
        bufferState = result.state;
        recordDrop(result.dropped);
      }
    } else {
      droppedCount += 1;
      oversizeDropCount += 1;
      return;
    }

    if (bufferState.entries.length >= minBatchSize) {
      void flush(false);
      return;
    }

    scheduleFlush();
  };

  const stop = async (): Promise<void> => {
    clearFlushTimer();
    await flush(true);
  };

  const stats = (): TransportStats =>
    Object.freeze({
      bufferedCount: bufferState.entries.length,
      bufferedBytes: bufferState.totalBytes,
      droppedCount,
      fragmentedCount,
      oversizeDropCount,
      jetStreamAvailable,
    });

  return Object.freeze({ write, flush, stop, stats });
}

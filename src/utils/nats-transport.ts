import type { NatsConnection } from 'nats';
import { getNats } from '../nats/connection';
import type { LogEnvelope, LogLevel } from '@insnh-gd/meristem-shared';

const DEFAULT_MAX_BUFFER_BYTES = 5 * 1024 * 1024;
const DEFAULT_MIN_BATCH_SIZE = 50;
const DEFAULT_MAX_BATCH_SIZE = 100;
const DEFAULT_FLUSH_INTERVAL_MS = 100;

const SYSTEM_TOPIC_PREFIX = 'meristem.v1.logs.sys.';
const TASK_TOPIC_PREFIX = 'meristem.v1.logs.task.';

export type { LogEnvelope };

type NatsConnectionLike = Pick<NatsConnection, 'publish' | 'jetstreamManager'>;

export type NatsTransportOptions = Readonly<{
  readonly bufferMaxBytes?: number;
  readonly minBatchSize?: number;
  readonly maxBatchSize?: number;
  readonly flushIntervalMs?: number;
  readonly getConnection?: () => Promise<NatsConnectionLike>;
  readonly encode?: (value: string) => Uint8Array;
}>;

export type TransportStats = Readonly<{
  readonly bufferedCount: number;
  readonly bufferedBytes: number;
  readonly droppedCount: number;
  readonly jetStreamAvailable: boolean | null;
}>;

export type NatsTransport = Readonly<{
  readonly write: (input: unknown) => void;
  readonly flush: (allowPartial?: boolean) => Promise<void>;
  readonly stop: () => Promise<void>;
  readonly stats: () => TransportStats;
}>;

type BufferedEntry = Readonly<{
  readonly subject: string;
  readonly payload: Uint8Array;
  readonly size: number;
}>;

type RingBufferState = Readonly<{
  readonly entries: readonly BufferedEntry[];
  readonly totalBytes: number;
  readonly maxBytes: number;
}>;

type BufferUpdate = Readonly<{
  readonly state: RingBufferState;
  readonly dropped: number;
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

const createRingBuffer = (maxBytes: number): RingBufferState =>
  Object.freeze({
    entries: Object.freeze([]) as readonly BufferedEntry[],
    totalBytes: 0,
    maxBytes,
  });

const sumEntryBytes = (entries: readonly BufferedEntry[]): number => {
  let total = 0;
  for (const entry of entries) {
    total += entry.size;
  }
  return total;
};

const appendEntry = (state: RingBufferState, entry: BufferedEntry): BufferUpdate => {
  if (entry.size > state.maxBytes) {
    return { state, dropped: 1 };
  }

  const combined = [...state.entries, entry];
  let totalBytes = state.totalBytes + entry.size;
  let dropCount = 0;
  let startIndex = 0;

  while (totalBytes > state.maxBytes && startIndex < combined.length) {
    totalBytes -= combined[startIndex].size;
    startIndex += 1;
    dropCount += 1;
  }

  const entries = startIndex > 0 ? combined.slice(startIndex) : combined;
  const nextState: RingBufferState = Object.freeze({
    entries: Object.freeze(entries) as readonly BufferedEntry[],
    totalBytes,
    maxBytes: state.maxBytes,
  });

  return { state: nextState, dropped: dropCount };
};

const takeBatch = (state: RingBufferState, size: number): { batch: readonly BufferedEntry[]; state: RingBufferState } => {
  if (state.entries.length === 0 || size <= 0) {
    return { batch: Object.freeze([]) as readonly BufferedEntry[], state };
  }

  const batch = state.entries.slice(0, size);
  const remaining = state.entries.slice(batch.length);
  const remainingBytes = state.totalBytes - sumEntryBytes(batch);
  const nextState: RingBufferState = Object.freeze({
    entries: Object.freeze(remaining) as readonly BufferedEntry[],
    totalBytes: remainingBytes,
    maxBytes: state.maxBytes,
  });

  return { batch: Object.freeze(batch) as readonly BufferedEntry[], state: nextState };
};

const prependBatch = (state: RingBufferState, batch: readonly BufferedEntry[]): RingBufferState => {
  if (batch.length === 0) {
    return state;
  }

  const entries = [...batch, ...state.entries];
  const totalBytes = state.totalBytes + sumEntryBytes(batch);
  return Object.freeze({
    entries: Object.freeze(entries) as readonly BufferedEntry[],
    totalBytes,
    maxBytes: state.maxBytes,
  });
};

const clampBatchSize = (value: number, fallback: number): number => {
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return value;
};

const createEntry = (subject: string, payload: Uint8Array): BufferedEntry =>
  Object.freeze({ subject, payload, size: payload.byteLength });

export function createNatsTransport(options: NatsTransportOptions = {}): NatsTransport {
  const bufferMaxBytes = options.bufferMaxBytes ?? DEFAULT_MAX_BUFFER_BYTES;
  const minBatchSize = clampBatchSize(options.minBatchSize ?? DEFAULT_MIN_BATCH_SIZE, DEFAULT_MIN_BATCH_SIZE);
  const resolvedMaxBatch = clampBatchSize(options.maxBatchSize ?? DEFAULT_MAX_BATCH_SIZE, DEFAULT_MAX_BATCH_SIZE);
  const maxBatchSize = Math.max(minBatchSize, resolvedMaxBatch);
  const flushIntervalMs = options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
  const getConnection = options.getConnection ?? getNats;
  const textEncoder = new TextEncoder();
  const encoder = options.encode ?? ((value: string) => textEncoder.encode(value));

  let bufferState = createRingBuffer(bufferMaxBytes);
  let droppedCount = 0;
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
        await connection.publish(entry.subject, entry.payload);
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

    const payload = encoder(serialized);
    const entry = createEntry(resolveSubject(envelope), payload);
    const result = appendEntry(bufferState, entry);
    bufferState = result.state;
    recordDrop(result.dropped);

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
      jetStreamAvailable,
    });

  return Object.freeze({ write, flush, stop, stats });
}

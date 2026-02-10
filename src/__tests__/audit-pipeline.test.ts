import { expect, test } from 'bun:test';
import type { Collection, Db } from 'mongodb';
import type { TraceContext } from '../utils/trace-context';
import {
  AUDIT_COLLECTION,
  AUDIT_STATE_COLLECTION,
  type AuditEventInput,
  type AuditLog,
} from '../services/audit';
import {
  AUDIT_FAILURES_COLLECTION,
  AUDIT_GLOBAL_ANCHOR_COLLECTION,
  AUDIT_INTENTS_COLLECTION,
  AUDIT_PARTITION_STATE_COLLECTION,
  drainAuditPipelineOnce,
  enqueueAuditIntent,
  recordAuditEvent,
  startAuditPipeline,
  stopAuditPipeline,
  type AuditFailureRecord,
  type AuditGlobalAnchor,
  type AuditIntent,
  type AuditPartitionState,
} from '../services/audit-pipeline';

type AuditSequenceState = {
  _id: string;
  value: number;
  global_last_sequence?: number;
  global_last_hash?: string;
};

type MemoryState = {
  logs: AuditLog[];
  state: AuditSequenceState[];
  intents: AuditIntent[];
  partitions: AuditPartitionState[];
  anchors: AuditGlobalAnchor[];
  failures: AuditFailureRecord[];
};

type CursorLike<T> = {
  sort: (spec: Record<string, 1 | -1>) => CursorLike<T>;
  limit: (count: number) => CursorLike<T>;
  toArray: () => Promise<T[]>;
};

const asRecord = (value: unknown): Record<string, unknown> =>
  (typeof value === 'object' && value !== null ? value : {}) as Record<string, unknown>;

const toComparableNumber = (value: unknown): number | null => {
  if (value instanceof Date) {
    return value.getTime();
  }
  if (typeof value === 'number') {
    return value;
  }
  return null;
};

const matchesFilter = (doc: Record<string, unknown>, filter?: Record<string, unknown>): boolean => {
  if (!filter) {
    return true;
  }
  for (const [key, condition] of Object.entries(filter)) {
    if (key === '$or' && Array.isArray(condition)) {
      const matched = condition.some((entry) => {
        if (typeof entry !== 'object' || entry === null) {
          return false;
        }
        return matchesFilter(doc, entry as Record<string, unknown>);
      });
      if (!matched) {
        return false;
      }
      continue;
    }

    const value = doc[key];
    if (typeof condition === 'object' && condition !== null) {
      const conditionRecord = condition as Record<string, unknown>;
      if (Array.isArray(conditionRecord.$in)) {
        if (!conditionRecord.$in.includes(value)) {
          return false;
        }
        continue;
      }
      if (typeof conditionRecord.$exists === 'boolean') {
        const exists = value !== undefined;
        if (exists !== conditionRecord.$exists) {
          return false;
        }
        continue;
      }
      if (conditionRecord.$lt !== undefined) {
        const left = toComparableNumber(value);
        const right = toComparableNumber(conditionRecord.$lt);
        if (left === null || right === null || left >= right) {
          return false;
        }
        continue;
      }
      if (conditionRecord.$lte !== undefined) {
        const left = toComparableNumber(value);
        const right = toComparableNumber(conditionRecord.$lte);
        if (left === null || right === null || left > right) {
          return false;
        }
        continue;
      }
    }
    if (value !== condition) {
      return false;
    }
  }
  return true;
};

const applySet = (
  doc: Record<string, unknown>,
  update: Record<string, unknown>,
): Record<string, unknown> => {
  const next = { ...doc };
  const set = update.$set;
  if (typeof set === 'object' && set !== null) {
    for (const [key, value] of Object.entries(set)) {
      next[key] = value;
    }
  }
  const setOnInsert = update.$setOnInsert;
  if (typeof setOnInsert === 'object' && setOnInsert !== null) {
    for (const [key, value] of Object.entries(setOnInsert)) {
      if (!(key in next)) {
        next[key] = value;
      }
    }
  }
  const inc = update.$inc;
  if (typeof inc === 'object' && inc !== null) {
    for (const [key, value] of Object.entries(inc)) {
      if (typeof value === 'number') {
        const current = typeof next[key] === 'number' ? (next[key] as number) : 0;
        next[key] = current + value;
      }
    }
  }
  const max = update.$max;
  if (typeof max === 'object' && max !== null) {
    for (const [key, value] of Object.entries(max)) {
      if (typeof value === 'number') {
        const current = typeof next[key] === 'number' ? (next[key] as number) : Number.NEGATIVE_INFINITY;
        next[key] = Math.max(current, value);
      }
    }
  }
  return next;
};

const createCursor = <T extends Record<string, unknown>>(rows: T[]): CursorLike<T> => {
  let snapshot = [...rows];
  return {
    sort: (spec: Record<string, 1 | -1>): CursorLike<T> => {
      const entries = Object.entries(spec);
      snapshot.sort((left, right) => {
        for (const [field, direction] of entries) {
          const leftValue = left[field];
          const rightValue = right[field];
          if (leftValue === rightValue) {
            continue;
          }
          if (leftValue === undefined) {
            return direction === 1 ? -1 : 1;
          }
          if (rightValue === undefined) {
            return direction === 1 ? 1 : -1;
          }
          if (leftValue !== null && rightValue !== null && leftValue < rightValue) {
            return direction === 1 ? -1 : 1;
          }
          if (leftValue !== null && rightValue !== null && leftValue > rightValue) {
            return direction === 1 ? 1 : -1;
          }
        }
        return 0;
      });
      return createCursor(snapshot);
    },
    limit: (count: number): CursorLike<T> => createCursor(snapshot.slice(0, count)),
    toArray: async (): Promise<T[]> => [...snapshot],
  };
};

const createDb = (state: MemoryState): Db => {
  const logsCollection = {
    findOne: async (filter?: Record<string, unknown>, options?: Record<string, unknown>): Promise<AuditLog | null> => {
      let rows = state.logs.filter((doc) => matchesFilter(asRecord(doc), filter));
      if (options?.sort && typeof options.sort === 'object') {
        rows = await createCursor(rows as unknown as Record<string, unknown>[])
          .sort(options.sort as Record<string, 1 | -1>)
          .toArray() as unknown as AuditLog[];
      }
      return rows[0] ?? null;
    },
    insertOne: async (doc: AuditLog): Promise<{ insertedId: string }> => {
      if (state.logs.some((log) => log._sequence === doc._sequence)) {
        throw { code: 11000 } as { code: number };
      }
      state.logs.push(doc);
      return { insertedId: `${doc._sequence}` };
    },
    find: (filter?: Record<string, unknown>): CursorLike<AuditLog> =>
      createCursor(state.logs.filter((doc) => matchesFilter(asRecord(doc), filter)) as unknown as Record<string, unknown>[]) as unknown as CursorLike<AuditLog>,
  };

  const stateCollection = {
    findOne: async (filter?: Record<string, unknown>): Promise<AuditSequenceState | null> =>
      state.state.find((row) => matchesFilter(asRecord(row), filter)) ?? null,
    insertOne: async (doc: AuditSequenceState): Promise<{ insertedId: string }> => {
      if (state.state.some((row) => row._id === doc._id)) {
        throw { code: 11000 } as { code: number };
      }
      state.state.push({ ...doc });
      return { insertedId: doc._id };
    },
    findOneAndUpdate: async (
      filter: Record<string, unknown>,
      update: Record<string, unknown>,
      options?: Record<string, unknown>,
    ): Promise<AuditSequenceState | null> => {
      const index = state.state.findIndex((row) => matchesFilter(asRecord(row), filter));
      if (index < 0) {
        if (options?.upsert === true) {
          const seed = applySet(
            {
              _id: typeof filter._id === 'string' ? filter._id : 'global',
              value: 0,
            },
            update,
          ) as AuditSequenceState;
          state.state.push(seed);
          return seed;
        }
        return null;
      }
      const next = applySet(state.state[index], update) as AuditSequenceState;
      state.state[index] = next;
      return next;
    },
    updateOne: async (
      filter: Record<string, unknown>,
      update: Record<string, unknown>,
      options?: Record<string, unknown>,
    ): Promise<{ modifiedCount: number; upsertedCount?: number }> => {
      const index = state.state.findIndex((row) => matchesFilter(asRecord(row), filter));
      if (index < 0) {
        if (options?.upsert === true) {
          const seed = applySet(
            {
              _id: typeof filter._id === 'string' ? filter._id : 'global',
              value: 0,
            },
            update,
          ) as AuditSequenceState;
          state.state.push(seed);
          return { modifiedCount: 0, upsertedCount: 1 };
        }
        return { modifiedCount: 0 };
      }
      state.state[index] = applySet(state.state[index], update) as AuditSequenceState;
      return { modifiedCount: 1 };
    },
  };

  const intentsCollection = {
    countDocuments: async (filter?: Record<string, unknown>): Promise<number> =>
      state.intents.filter((doc) => matchesFilter(asRecord(doc), filter)).length,
    insertOne: async (doc: AuditIntent): Promise<{ insertedId: string }> => {
      if (state.intents.some((intent) => intent.event_id === doc.event_id)) {
        throw { code: 11000 } as { code: number };
      }
      state.intents.push(doc);
      return { insertedId: doc.event_id };
    },
    find: (filter?: Record<string, unknown>): CursorLike<AuditIntent> =>
      createCursor(state.intents.filter((doc) => matchesFilter(asRecord(doc), filter)) as unknown as Record<string, unknown>[]) as unknown as CursorLike<AuditIntent>,
    updateOne: async (
      filter: Record<string, unknown>,
      update: Record<string, unknown>,
    ): Promise<{ modifiedCount: number }> => {
      const index = state.intents.findIndex((doc) => matchesFilter(asRecord(doc), filter));
      if (index < 0) {
        return { modifiedCount: 0 };
      }
      state.intents[index] = applySet(state.intents[index], update) as AuditIntent;
      return { modifiedCount: 1 };
    },
  };

  const partitionStateCollection = {
    findOne: async (filter?: Record<string, unknown>): Promise<AuditPartitionState | null> =>
      state.partitions.find((row) => matchesFilter(asRecord(row), filter)) ?? null,
    updateOne: async (
      filter: Record<string, unknown>,
      update: Record<string, unknown>,
      options?: Record<string, unknown>,
    ): Promise<{ modifiedCount: number; upsertedCount?: number }> => {
      const index = state.partitions.findIndex((row) => matchesFilter(asRecord(row), filter));
      if (index < 0) {
        if (options?.upsert === true) {
          const seed = applySet(
            {
              _id: typeof filter._id === 'string' ? filter._id : 'partition:0',
              partition_id: typeof filter.partition_id === 'number' ? filter.partition_id : 0,
              last_sequence: 0,
              last_hash: '',
              updated_at: new Date(),
            },
            update,
          ) as AuditPartitionState;
          state.partitions.push(seed);
          return { modifiedCount: 0, upsertedCount: 1 };
        }
        return { modifiedCount: 0 };
      }
      state.partitions[index] = applySet(state.partitions[index], update) as AuditPartitionState;
      return { modifiedCount: 1 };
    },
    find: (): CursorLike<AuditPartitionState> =>
      createCursor(state.partitions as unknown as Record<string, unknown>[]) as unknown as CursorLike<AuditPartitionState>,
  };

  const anchorsCollection = {
    insertOne: async (doc: AuditGlobalAnchor): Promise<{ insertedId: string }> => {
      state.anchors.push(doc);
      return { insertedId: doc.anchor_id };
    },
    findOne: async (_filter?: Record<string, unknown>, options?: Record<string, unknown>): Promise<AuditGlobalAnchor | null> => {
      if (state.anchors.length === 0) {
        return null;
      }
      if (options?.sort && typeof options.sort === 'object') {
        const rows = await createCursor(state.anchors as unknown as Record<string, unknown>[])
          .sort(options.sort as Record<string, 1 | -1>)
          .toArray() as unknown as AuditGlobalAnchor[];
        return rows[0] ?? null;
      }
      return state.anchors[state.anchors.length - 1] ?? null;
    },
  };

  const failuresCollection = {
    insertOne: async (doc: AuditFailureRecord): Promise<{ insertedId: string }> => {
      state.failures.push(doc);
      return { insertedId: doc.event_id };
    },
  };

  return {
    collection: (name: string): Collection => {
      if (name === AUDIT_COLLECTION) {
        return logsCollection as unknown as Collection;
      }
      if (name === AUDIT_STATE_COLLECTION) {
        return stateCollection as unknown as Collection;
      }
      if (name === AUDIT_INTENTS_COLLECTION) {
        return intentsCollection as unknown as Collection;
      }
      if (name === AUDIT_PARTITION_STATE_COLLECTION) {
        return partitionStateCollection as unknown as Collection;
      }
      if (name === AUDIT_GLOBAL_ANCHOR_COLLECTION) {
        return anchorsCollection as unknown as Collection;
      }
      if (name === AUDIT_FAILURES_COLLECTION) {
        return failuresCollection as unknown as Collection;
      }
      throw new Error(`unexpected collection: ${name}`);
    },
  } as unknown as Db;
};

const TRACE_CONTEXT: TraceContext = {
  traceId: 'audit-pipeline-test',
  nodeId: 'core-test',
  source: 'test',
};

test('recordAuditEvent falls back to synchronous hash-chain when pipeline is stopped', async (): Promise<void> => {
  await stopAuditPipeline();
  const state: MemoryState = {
    logs: [],
    state: [],
    intents: [],
    partitions: [],
    anchors: [],
    failures: [],
  };
  const db = createDb(state);
  const event: AuditEventInput = {
    ts: 1700000000000,
    level: 'INFO',
    node_id: 'node-sync',
    source: 'join',
    trace_id: 'trace-sync',
    content: 'sync fallback',
    meta: { mode: 'sync' },
  };

  const log = await recordAuditEvent(db, event, { routeTag: 'join' });
  expect(log?._sequence).toBe(1);
  expect(state.logs).toHaveLength(1);
  expect(state.intents).toHaveLength(0);
});

test('enqueueAuditIntent + drainAuditPipelineOnce commits queued intents into audit_logs', async (): Promise<void> => {
  const state: MemoryState = {
    logs: [],
    state: [],
    intents: [],
    partitions: [],
    anchors: [],
    failures: [],
  };
  const db = createDb(state);
  await startAuditPipeline(db, TRACE_CONTEXT, {
    enableBackgroundLoops: false,
    partitionCount: 4,
    batchSize: 16,
    flushIntervalMs: 10,
    anchorIntervalMs: 1_000,
  });

  const event: AuditEventInput = {
    ts: 1700000000100,
    level: 'INFO',
    node_id: 'node-async',
    source: 'tasks',
    trace_id: 'trace-async',
    content: 'queued',
    meta: { mode: 'async' },
  };

  const accepted = await enqueueAuditIntent(db, event, { routeTag: 'tasks' });
  expect(accepted.accepted).toBe(true);
  expect(state.intents).toHaveLength(1);

  await drainAuditPipelineOnce(db, TRACE_CONTEXT);

  expect(state.logs).toHaveLength(1);
  expect(state.logs[0]?._sequence).toBe(1);
  expect(typeof state.logs[0]?.event_id).toBe('string');
  expect(state.intents[0]?.status).toBe('committed');
  expect(state.failures).toHaveLength(0);

  await stopAuditPipeline();
});

test('drainAuditPipelineOnce reclaims expired processing intents and commits them', async (): Promise<void> => {
  const state: MemoryState = {
    logs: [],
    state: [],
    intents: [],
    partitions: [],
    anchors: [],
    failures: [],
  };
  const db = createDb(state);
  await startAuditPipeline(db, TRACE_CONTEXT, {
    enableBackgroundLoops: false,
    partitionCount: 4,
    batchSize: 16,
  });

  const event: AuditEventInput = {
    ts: 1700000000200,
    level: 'INFO',
    node_id: 'node-stale',
    source: 'join',
    trace_id: 'trace-stale',
    content: 'stale processing intent',
    meta: { mode: 'stale-lease' },
  };

  const intake = await enqueueAuditIntent(db, event, { routeTag: 'join' });
  expect(intake.accepted).toBe(true);
  expect(state.intents).toHaveLength(1);

  const firstIntent = state.intents[0];
  if (!firstIntent) {
    throw new Error('expected queued intent');
  }
  state.intents[0] = {
    ...firstIntent,
    status: 'processing',
    lease_owner: 'stale-worker',
    lease_until: new Date(Date.now() - 1_000),
  };

  await drainAuditPipelineOnce(db, TRACE_CONTEXT);

  expect(state.logs).toHaveLength(1);
  expect(state.logs[0]?._sequence).toBe(1);
  expect(state.intents[0]?.status).toBe('committed');
  expect(state.failures).toHaveLength(0);

  await stopAuditPipeline();
});

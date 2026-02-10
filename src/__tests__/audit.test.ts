import { test, expect } from 'bun:test';
import type { Collection, Db } from 'mongodb';
import {
  AUDIT_COLLECTION,
  AUDIT_STATE_COLLECTION,
  AuditEventInput,
  AuditLog,
  calculateHash,
  logAuditEvent,
  resetAuditState,
  verifyChain,
} from '../services/audit';

type AuditSequenceState = {
  _id: string;
  value: number;
};

type MockDbOptions = {
  insertDelayMs?: (sequence: number) => number;
};

const sleep = async (durationMs: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });

const createMockDb = (options: MockDbOptions = {}): { db: Db; insertedLogs: AuditLog[] } => {
  const insertedLogs: AuditLog[] = [];
  let sequenceState: AuditSequenceState | null = null;
  const getInsertDelayMs = options.insertDelayMs ?? (() => 0);

  const auditCollection = {
    findOne: async (query?: Record<string, unknown>): Promise<AuditLog | null> => {
      const sequence = typeof query?._sequence === 'number' ? query._sequence : null;
      if (sequence === null) {
        return insertedLogs[insertedLogs.length - 1] ?? null;
      }

      return insertedLogs.find((log) => log._sequence === sequence) ?? null;
    },
    insertOne: async (doc: AuditLog): Promise<{ insertedId: string }> => {
      const delayMs = getInsertDelayMs(doc._sequence);
      if (delayMs > 0) {
        await sleep(delayMs);
      }

      insertedLogs.push(doc);
      return { insertedId: `${doc._sequence}` };
    },
  };

  const sequenceCollection = {
    findOne: async (): Promise<AuditSequenceState | null> => sequenceState,
    insertOne: async (doc: AuditSequenceState): Promise<{ insertedId: string }> => {
      if (sequenceState !== null) {
        throw { code: 11000 } as { code: number };
      }
      sequenceState = { ...doc };
      return { insertedId: doc._id };
    },
    findOneAndUpdate: async (
      _filter: Record<string, unknown>,
      update: Record<string, unknown>,
    ): Promise<AuditSequenceState> => {
      if (sequenceState === null) {
        sequenceState = {
          _id: 'global',
          value: 0,
        };
      }
      const increment =
        typeof update.$inc === 'object' &&
        update.$inc !== null &&
        typeof (update.$inc as { value?: unknown }).value === 'number'
          ? ((update.$inc as { value: number }).value)
          : 0;
      sequenceState = {
        ...sequenceState,
        value: sequenceState.value + increment,
      };
      return { ...sequenceState };
    },
  };

  const db = {
    collection: (name: string): Collection<AuditLog> | Collection<AuditSequenceState> => {
      if (name === AUDIT_COLLECTION) {
        return auditCollection as unknown as Collection<AuditLog>;
      }

      if (name === AUDIT_STATE_COLLECTION) {
        return sequenceCollection as unknown as Collection<AuditSequenceState>;
      }

      throw new Error(`unexpected collection in audit test: ${name}`);
    },
  } as unknown as Db;

  return { db, insertedLogs };
};

// 验证 calculateHash 对稳定输入返回预期的 SHA-256
test('calculateHash returns deterministic digest', async (): Promise<void> => {
  const sampleLog: AuditLog = {
    ts: 1670000000000,
    level: 'INFO',
    node_id: 'node-test-1',
    source: 'core',
    trace_id: 'trace-test',
    content: 'audit check',
    meta: { step: 'hash-check' },
    _sequence: 1,
    _hash: '',
    _previous_hash: '',
  };

  const digest = calculateHash(sampleLog);
  expect(digest).toBe('78f0f260057c9770c0037a8cd206a8b426fa76833ff6060f01eabe7ce9fb17be');
});

// 验证 verifyChain 能识别连续序列并保持哈希链完整性
test('verifyChain approves valid hash chains', async (): Promise<void> => {
  const firstLog: AuditLog = {
    ts: 1670000000001,
    level: 'INFO',
    node_id: 'node-valid-1',
    source: 'core',
    trace_id: 'trace-chain',
    content: 'first entry',
    meta: { step: 'first' },
    _sequence: 1,
    _hash: '',
    _previous_hash: '',
  };
  firstLog._hash = calculateHash(firstLog);

  const secondLog: AuditLog = {
    ts: 1670000000002,
    level: 'INFO',
    node_id: 'node-valid-1',
    source: 'core',
    trace_id: 'trace-chain',
    content: 'second entry',
    meta: { step: 'second' },
    _sequence: 2,
    _hash: '',
    _previous_hash: firstLog._hash,
  };
  secondLog._hash = calculateHash(secondLog);

  const result = verifyChain([firstLog, secondLog]);
  expect(result.valid).toBe(true);
  expect(result.error).toBeUndefined();
});

// 验证 logAuditEvent 会递增序列号并将 _previous_hash 链接到前一条日志
test('logAuditEvent links hashes and increments sequence', async (): Promise<void> => {
  const { db, insertedLogs } = createMockDb();

  resetAuditState();

  const firstEvent: AuditEventInput = {
    ts: 1670000001000,
    level: 'INFO',
    node_id: 'node-event-1',
    source: 'core',
    trace_id: 'trace-event',
    content: 'start logging',
    meta: { action: 'start' },
  };
  const firstResult = await logAuditEvent(db, firstEvent);
  expect(firstResult._sequence).toBe(1);
  expect(firstResult._previous_hash).toBe('');

  const secondEvent: AuditEventInput = {
    ts: 1670000002000,
    level: 'INFO',
    node_id: 'node-event-1',
    source: 'core',
    trace_id: 'trace-event',
    content: 'continue logging',
    meta: { action: 'continue' },
  };
  const secondResult = await logAuditEvent(db, secondEvent);
  expect(secondResult._sequence).toBe(2);
  expect(secondResult._previous_hash).toBe(firstResult._hash);
  expect(insertedLogs[1]).toEqual(secondResult);
});

test('logAuditEvent waits predecessor when inserts finish out of order', async (): Promise<void> => {
  const { db } = createMockDb({
    insertDelayMs: (sequence) => {
      if (sequence === 1) {
        return 25;
      }
      return 0;
    },
  });
  resetAuditState();

  const firstEvent: AuditEventInput = {
    ts: 1670000003000,
    level: 'INFO',
    node_id: 'node-wait-1',
    source: 'core',
    trace_id: 'trace-wait',
    content: 'first',
    meta: { action: 'first' },
  };
  const secondEvent: AuditEventInput = {
    ts: 1670000003001,
    level: 'INFO',
    node_id: 'node-wait-1',
    source: 'core',
    trace_id: 'trace-wait',
    content: 'second',
    meta: { action: 'second' },
  };

  const firstPromise = logAuditEvent(db, firstEvent);
  await sleep(1);
  const secondPromise = logAuditEvent(db, secondEvent);

  const [firstResult, secondResult] = await Promise.all([firstPromise, secondPromise]);
  expect(secondResult._sequence).toBe(2);
  expect(secondResult._previous_hash).toBe(firstResult._hash);
});

test('logAuditEvent waits for delayed predecessor beyond short retry windows', async (): Promise<void> => {
  const { db } = createMockDb({
    insertDelayMs: (sequence) => {
      if (sequence === 1) {
        return 380;
      }
      return 0;
    },
  });
  resetAuditState();

  const firstEvent: AuditEventInput = {
    ts: 1670000004000,
    level: 'INFO',
    node_id: 'node-delay-1',
    source: 'core',
    trace_id: 'trace-delay',
    content: 'first-delayed',
    meta: { action: 'first-delayed' },
  };
  const secondEvent: AuditEventInput = {
    ts: 1670000004001,
    level: 'INFO',
    node_id: 'node-delay-1',
    source: 'core',
    trace_id: 'trace-delay',
    content: 'second-delayed',
    meta: { action: 'second-delayed' },
  };

  const firstPromise = logAuditEvent(db, firstEvent);
  await sleep(1);
  const secondPromise = logAuditEvent(db, secondEvent);

  const [firstResult, secondResult] = await Promise.all([firstPromise, secondPromise]);
  expect(secondResult._sequence).toBe(2);
  expect(secondResult._previous_hash).toBe(firstResult._hash);
});

test('logAuditEvent keeps chain integrity under high concurrency', async (): Promise<void> => {
  const { db, insertedLogs } = createMockDb({
    insertDelayMs: (sequence) => {
      if (sequence % 7 === 0) {
        return 4;
      }
      if (sequence % 3 === 0) {
        return 2;
      }
      return 0;
    },
  });
  resetAuditState();

  const events: AuditEventInput[] = Array.from({ length: 100 }, (_, index) => ({
    ts: 1670000010000 + index,
    level: 'INFO',
    node_id: 'node-concurrency',
    source: 'core',
    trace_id: 'trace-concurrency',
    content: `event-${index + 1}`,
    meta: { sequence_hint: index + 1 },
  }));

  await Promise.all(events.map((event) => logAuditEvent(db, event)));

  const sortedLogs = [...insertedLogs].sort((left, right) => left._sequence - right._sequence);
  const sequenceList = sortedLogs.map((log) => log._sequence);

  expect(sortedLogs).toHaveLength(100);
  expect(sequenceList).toEqual(Array.from({ length: 100 }, (_, index) => index + 1));
  expect(new Set(sequenceList).size).toBe(100);

  const verification = verifyChain(sortedLogs);
  expect(verification.valid).toBe(true);
  expect(verification.error).toBeUndefined();
});

test('logAuditEvent initializes sequence state from latest audit log on cold start', async (): Promise<void> => {
  const { db, insertedLogs } = createMockDb();
  insertedLogs.push({
    ts: 1670000020000,
    level: 'INFO',
    node_id: 'node-cold-start',
    source: 'core',
    trace_id: 'trace-cold-start',
    content: 'existing-log',
    meta: { action: 'existing' },
    _sequence: 7,
    _hash: '',
    _previous_hash: '',
  });
  insertedLogs[0]._hash = calculateHash(insertedLogs[0]);

  resetAuditState();
  const event: AuditEventInput = {
    ts: 1670000021000,
    level: 'INFO',
    node_id: 'node-cold-start',
    source: 'core',
    trace_id: 'trace-cold-start',
    content: 'new-log',
    meta: { action: 'new' },
  };

  const result = await logAuditEvent(db, event);
  expect(result._sequence).toBe(8);
  expect(result._previous_hash).toBe(insertedLogs[0]._hash);
});

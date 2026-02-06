import { test, expect } from 'bun:test';
import type { Collection, Db } from 'mongodb';
import {
  AUDIT_COLLECTION,
  AuditEventInput,
  AuditLog,
  calculateHash,
  logAuditEvent,
  resetAuditState,
  verifyChain,
} from '../services/audit';

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
  const insertedLogs: AuditLog[] = [];

  const auditCollection = {
    findOne: async (
      _query?: Record<string, unknown>,
      options?: { sort?: { _sequence: -1 | 1 } },
    ): Promise<AuditLog | null> => {
      if (options?.sort?._sequence === -1) {
        return insertedLogs[insertedLogs.length - 1] ?? null;
      }
      return insertedLogs[0] ?? null;
    },
    insertOne: async (doc: AuditLog): Promise<{ insertedId: string }> => {
      insertedLogs.push(doc);
      return { insertedId: 'mock-id' };
    },
  };

  const mockDb = {
    collection: (_name: string): Collection<AuditLog> => {
      return auditCollection as unknown as Collection<AuditLog>;
    },
  };

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
  const firstResult = await logAuditEvent(mockDb as Db, firstEvent);
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
  const secondResult = await logAuditEvent(mockDb as Db, secondEvent);
  expect(secondResult._sequence).toBe(2);
  expect(secondResult._previous_hash).toBe(firstResult._hash);
  expect(insertedLogs[1]).toEqual(secondResult);
});

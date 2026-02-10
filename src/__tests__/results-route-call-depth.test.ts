import { expect, test } from 'bun:test';
import { Elysia } from 'elysia';
import type { Collection, Db, Document } from 'mongodb';

import { resultsRoute } from '../routes/results';
import {
  AUDIT_COLLECTION,
  AUDIT_STATE_COLLECTION,
  resetAuditState,
  type AuditLog,
} from '../services/audit';

type TestState = {
  audits: AuditLog[];
  sequence: number;
};

const createMockDb = (state: TestState): Db => {
  const auditCollection = {
    findOne: async (query?: Record<string, unknown>): Promise<AuditLog | null> => {
      const sequence = typeof query?._sequence === 'number' ? query._sequence : null;
      if (sequence === null) {
        return state.audits[state.audits.length - 1] ?? null;
      }
      return state.audits.find((log) => log._sequence === sequence) ?? null;
    },
    insertOne: async (doc: AuditLog): Promise<{ insertedId: string }> => {
      state.audits.push(doc);
      return { insertedId: `${doc._sequence}` };
    },
  };

  const auditStateCollection = {
    findOne: async (): Promise<{ _id: string; value: number } | null> => {
      if (state.sequence === 0) {
        return null;
      }
      return { _id: 'global', value: state.sequence };
    },
    insertOne: async (): Promise<{ insertedId: string }> => {
      if (state.sequence !== 0) {
        throw { code: 11000 } as { code: number };
      }
      return { insertedId: 'global' };
    },
    findOneAndUpdate: async (): Promise<{ _id: string; value: number }> => {
      state.sequence += 1;
      return { _id: 'global', value: state.sequence };
    },
  };

  const db = {
    collection: <TSchema extends Document>(name: string): Collection<TSchema> => {
      if (name === AUDIT_COLLECTION) {
        return auditCollection as unknown as Collection<TSchema>;
      }
      if (name === AUDIT_STATE_COLLECTION) {
        return auditStateCollection as unknown as Collection<TSchema>;
      }
      throw new Error(`Unexpected collection: ${name}`);
    },
  };

  return db as unknown as Db;
};

test('results route rejects invalid call depth header before processing result', async (): Promise<void> => {
  resetAuditState();
  const state: TestState = { audits: [], sequence: 0 };
  const db = createMockDb(state);

  const app = new Elysia();
  resultsRoute(app, db);

  const response = await app.handle(
    new Request('http://localhost/api/v1/results', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-call-depth': 'depth-1',
      },
      body: JSON.stringify({
        task_id: 'task-1',
        status: 'completed',
      }),
    }),
  );

  const payload = await response.json();
  expect(response.status).toBe(400);
  expect(payload).toEqual({
    success: false,
    error: 'INVALID_CALL_DEPTH',
  });
  expect(state.audits).toHaveLength(1);
  expect(state.audits[0]?.content).toContain('invalid call_depth');
});

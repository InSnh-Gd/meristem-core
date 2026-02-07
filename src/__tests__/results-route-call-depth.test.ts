import { expect, test } from 'bun:test';
import { Elysia } from 'elysia';
import type { Collection, Db, Document } from 'mongodb';

import { resultsRoute } from '../routes/results';
import { AUDIT_COLLECTION, resetAuditState, type AuditLog } from '../services/audit';

type TestState = {
  audits: AuditLog[];
};

const createMockDb = (state: TestState): Db => {
  const auditCollection = {
    findOne: async (): Promise<AuditLog | null> => state.audits[state.audits.length - 1] ?? null,
    insertOne: async (doc: AuditLog): Promise<{ insertedId: string }> => {
      state.audits.push(doc);
      return { insertedId: `${doc._sequence}` };
    },
  };

  const db = {
    collection: <TSchema extends Document>(name: string): Collection<TSchema> => {
      if (name === AUDIT_COLLECTION) {
        return auditCollection as unknown as Collection<TSchema>;
      }
      throw new Error(`Unexpected collection: ${name}`);
    },
  };

  return db as unknown as Db;
};

test('results route rejects invalid call depth header before processing result', async (): Promise<void> => {
  resetAuditState();
  const state: TestState = { audits: [] };
  (global as { db?: Db }).db = createMockDb(state);

  const app = new Elysia();
  resultsRoute(app);

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

  delete (global as { db?: Db }).db;
});

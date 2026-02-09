import { expect, test } from 'bun:test';
import type { Db } from 'mongodb';
import {
  INVITATIONS_COLLECTION,
  NODES_COLLECTION,
  ORGS_COLLECTION,
  ROLES_COLLECTION,
  TASKS_COLLECTION,
  USERS_COLLECTION,
} from '../db/collections';
import { ensureDbIndexes, type IndexSpec } from '../db/indexes';
import type { TraceContext } from '../utils/trace-context';

type IndexCall = {
  collection: string;
  specs: readonly IndexSpec[];
};

const createDbMock = (calls: IndexCall[]): Db => {
  const collections = new Map<string, { createIndexes: (specs: readonly IndexSpec[]) => Promise<void> }>();
  const getCollection = (collection: string): { createIndexes: (specs: readonly IndexSpec[]) => Promise<void> } => {
    const existing = collections.get(collection);
    if (existing) {
      return existing;
    }

    const next = {
      createIndexes: async (specs: readonly IndexSpec[]): Promise<void> => {
        calls.push({ collection, specs });
      },
    };
    collections.set(collection, next);
    return next;
  };

  return {
    collection: (name: string) => getCollection(name),
  } as unknown as Db;
};

test('ensureDbIndexes creates expected unique and query indexes', async (): Promise<void> => {
  const calls: IndexCall[] = [];
  const db = createDbMock(calls);
  const traceContext: TraceContext = {
    traceId: 'test-indexes',
    nodeId: 'core-test',
    source: 'unit-test',
  };

  await ensureDbIndexes(db, traceContext);

  const collectionNames = calls.map((entry) => entry.collection);
  expect(collectionNames).toEqual(expect.arrayContaining([
    USERS_COLLECTION,
    ROLES_COLLECTION,
    TASKS_COLLECTION,
    NODES_COLLECTION,
    ORGS_COLLECTION,
    INVITATIONS_COLLECTION,
  ]));

  const usersIndexes = calls.find((entry) => entry.collection === USERS_COLLECTION)?.specs ?? [];
  const rolesIndexes = calls.find((entry) => entry.collection === ROLES_COLLECTION)?.specs ?? [];
  const tasksIndexes = calls.find((entry) => entry.collection === TASKS_COLLECTION)?.specs ?? [];
  const nodesIndexes = calls.find((entry) => entry.collection === NODES_COLLECTION)?.specs ?? [];
  const invitationsIndexes = calls.find((entry) => entry.collection === INVITATIONS_COLLECTION)?.specs ?? [];

  expect(usersIndexes).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ key: { user_id: 1 }, unique: true }),
      expect.objectContaining({ key: { username: 1 }, unique: true }),
      expect.objectContaining({ key: { org_id: 1, created_at: 1 } }),
    ]),
  );

  expect(rolesIndexes).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ key: { role_id: 1 }, unique: true }),
      expect.objectContaining({ key: { org_id: 1, name: 1 }, unique: true }),
    ]),
  );

  expect(tasksIndexes).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ key: { task_id: 1 }, unique: true }),
      expect.objectContaining({ key: { org_id: 1, created_at: 1 } }),
      expect.objectContaining({ key: { 'status.type': 1, target_node_id: 1, created_at: 1 } }),
    ]),
  );

  expect(nodesIndexes).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ key: { node_id: 1 }, unique: true }),
      expect.objectContaining({ key: { hwid: 1 }, unique: true }),
      expect.objectContaining({ key: { 'status.online': 1, 'status.last_seen': 1 } }),
    ]),
  );

  expect(invitationsIndexes).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ key: { invitation_id: 1 }, unique: true }),
      expect.objectContaining({ key: { invitation_token: 1 }, unique: true }),
      expect.objectContaining({ key: { status: 1, expires_at: 1 } }),
    ]),
  );
});

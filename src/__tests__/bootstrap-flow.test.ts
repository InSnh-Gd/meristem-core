import { afterEach, beforeEach, expect, test } from 'bun:test';
import { Elysia } from 'elysia';
import type { Collection, Db, Document } from 'mongodb';

import type { OrgDocument, RoleDocument, TaskDocument, UserDocument } from '../db/collections';
import { ORGS_COLLECTION, ROLES_COLLECTION } from '../db/collections';
import { authRoute } from '../routes/auth';
import { bootstrapRoute } from '../routes/bootstrap';
import { tasksRoute } from '../routes/tasks';
import { AUDIT_COLLECTION, resetAuditState, type AuditLog } from '../services/audit';

type TestState = {
  users: UserDocument[];
  roles: RoleDocument[];
  orgs: OrgDocument[];
  tasks: TaskDocument[];
  audits: AuditLog[];
};

const USERS_COLLECTION = 'users';
const TASKS_COLLECTION = 'tasks';
const JWT_SECRET_VALUE = 'bootstrap-flow-secret';

const asRecord = (value: unknown): Record<string, unknown> => value as Record<string, unknown>;

const createMockDb = (state: TestState): Db => {
  const usersCollection = {
    countDocuments: async (): Promise<number> => state.users.length,
    insertOne: async (doc: UserDocument): Promise<{ insertedId: string }> => {
      state.users.push(doc);
      return { insertedId: doc.user_id };
    },
    findOne: async (query?: Record<string, unknown>): Promise<UserDocument | null> => {
      const username = query?.username;
      if (typeof username !== 'string') {
        const userId = query?.user_id;
        if (typeof userId !== 'string') {
          return null;
        }
        return state.users.find((user) => user.user_id === userId) ?? null;
      }
      return state.users.find((user) => user.username === username) ?? null;
    },
  };

  const tasksCollection = {
    insertOne: async (doc: TaskDocument): Promise<{ insertedId: string }> => {
      state.tasks.push(doc);
      return { insertedId: doc.task_id };
    },
  };

  const auditCollection = {
    findOne: async (): Promise<AuditLog | null> => state.audits[state.audits.length - 1] ?? null,
    insertOne: async (doc: AuditLog): Promise<{ insertedId: string }> => {
      state.audits.push(doc);
      return { insertedId: `${doc._sequence}` };
    },
  };

  const rolesCollection = {
    findOne: async (query?: Record<string, unknown>): Promise<RoleDocument | null> => {
      const roleId = query?.role_id;
      if (typeof roleId !== 'string') {
        return null;
      }
      return state.roles.find((role) => role.role_id === roleId) ?? null;
    },
    insertOne: async (doc: RoleDocument): Promise<{ insertedId: string }> => {
      state.roles.push(doc);
      return { insertedId: doc.role_id };
    },
  };

  const orgsCollection = {
    findOne: async (query?: Record<string, unknown>): Promise<OrgDocument | null> => {
      const orgId = query?.org_id;
      if (typeof orgId !== 'string') {
        return null;
      }
      return state.orgs.find((org) => org.org_id === orgId) ?? null;
    },
    insertOne: async (doc: OrgDocument): Promise<{ insertedId: string }> => {
      state.orgs.push(doc);
      return { insertedId: doc.org_id };
    },
    updateOne: async (
      query: Record<string, unknown>,
      update: { $set?: Record<string, unknown> },
    ): Promise<{ modifiedCount: number }> => {
      const orgId = query.org_id;
      if (typeof orgId !== 'string') {
        return { modifiedCount: 0 };
      }
      const index = state.orgs.findIndex((org) => org.org_id === orgId);
      if (index < 0) {
        return { modifiedCount: 0 };
      }
      if (update.$set) {
        const next = { ...state.orgs[index] } as Record<string, unknown>;
        for (const [key, value] of Object.entries(update.$set)) {
          next[key] = value;
        }
        state.orgs[index] = next as OrgDocument;
      }
      return { modifiedCount: 1 };
    },
  };

  const db = {
    collection: <TSchema extends Document>(name: string): Collection<TSchema> => {
      if (name === USERS_COLLECTION) {
        return usersCollection as unknown as Collection<TSchema>;
      }
      if (name === ROLES_COLLECTION) {
        return rolesCollection as unknown as Collection<TSchema>;
      }
      if (name === ORGS_COLLECTION) {
        return orgsCollection as unknown as Collection<TSchema>;
      }
      if (name === TASKS_COLLECTION) {
        return tasksCollection as unknown as Collection<TSchema>;
      }
      if (name === AUDIT_COLLECTION) {
        return auditCollection as unknown as Collection<TSchema>;
      }
      throw new Error(`Unexpected collection: ${name}`);
    },
  };

  return db as unknown as Db;
};

const buildApp = (db: Db): Elysia => {
  const app = new Elysia();
  bootstrapRoute(app, db);
  authRoute(app, db);
  tasksRoute(app, db);
  return app;
};

const bootstrap = async (app: Elysia, username: string, password: string): Promise<Response> =>
  app.handle(
    new Request('http://localhost/api/v1/auth/bootstrap', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        bootstrap_token: 'ST-ABCD-1234',
        username,
        password,
      }),
    }),
  );

const login = async (app: Elysia, username: string, password: string): Promise<Response> =>
  app.handle(
    new Request('http://localhost/api/v1/auth/login', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        username,
        password,
      }),
    }),
  );

const createProtectedTask = async (app: Elysia, accessToken?: string, callDepth?: string): Promise<Response> => {
  const headers: HeadersInit = {
    'content-type': 'application/json',
  };
  if (accessToken) {
    headers.authorization = `Bearer ${accessToken}`;
  }
  if (callDepth !== undefined) {
    headers['x-call-depth'] = callDepth;
  }

  return app.handle(
    new Request('http://localhost/api/v1/tasks', {
      method: 'POST',
      headers,
      body: JSON.stringify({
        name: 'bootstrap-flow-task',
        payload: {
          action: 'noop',
        },
      }),
    }),
  );
};

const originalJwtSignSecret = process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;

beforeEach((): void => {
  resetAuditState();
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = JWT_SECRET_VALUE;
});

afterEach((): void => {
  if (originalJwtSignSecret === undefined) {
    delete process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;
  } else {
    process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = originalJwtSignSecret;
  }
});

test('fresh db bootstrap -> login -> protected task creation succeeds', async (): Promise<void> => {
  const state: TestState = { users: [], roles: [], orgs: [], tasks: [], audits: [] };
  const db = createMockDb(state);
  const app = buildApp(db);

  const bootstrapResponse = await bootstrap(app, 'admin', 'S3curePass!');
  const bootstrapPayload = asRecord(await bootstrapResponse.json());
  expect(bootstrapResponse.status).toBe(201);
  expect(bootstrapPayload.success).toBe(true);
  expect(typeof bootstrapPayload.user_id).toBe('string');

  const loginResponse = await login(app, 'admin', 'S3curePass!');
  const loginPayload = asRecord(await loginResponse.json());
  expect(loginResponse.status).toBe(200);
  expect(loginPayload.success).toBe(true);
  expect(loginPayload.token_type).toBe('Bearer');
  expect(typeof loginPayload.access_token).toBe('string');

  const taskResponse = await createProtectedTask(app, loginPayload.access_token as string);
  const taskPayload = asRecord(await taskResponse.json());
  expect(taskResponse.status).toBe(201);
  expect(taskPayload.success).toBe(true);
  expect(typeof taskPayload.task_id).toBe('string');
  expect(state.tasks).toHaveLength(1);
}, 20_000);

test('bootstrap creates user with org scope and role bindings', async (): Promise<void> => {
  const state: TestState = { users: [], roles: [], orgs: [], tasks: [], audits: [] };
  const db = createMockDb(state);
  const app = buildApp(db);

  const bootstrapResponse = await bootstrap(app, 'admin', 'S3curePass!');
  expect(bootstrapResponse.status).toBe(201);
  expect(state.users).toHaveLength(1);

  const user = state.users[0] as unknown as Record<string, unknown>;
  expect(typeof user.org_id).toBe('string');
  expect(Array.isArray(user.role_ids)).toBe(true);
  expect((user.role_ids as unknown[]).length).toBeGreaterThan(0);
  expect(Array.isArray(user.permissions)).toBe(true);
}, 20_000);

test('second bootstrap attempt is rejected after first user exists', async (): Promise<void> => {
  const state: TestState = { users: [], roles: [], orgs: [], tasks: [], audits: [] };
  const db = createMockDb(state);
  const app = buildApp(db);

  const firstResponse = await bootstrap(app, 'admin-a', 'S3curePass!');
  expect(firstResponse.status).toBe(201);

  const secondResponse = await bootstrap(app, 'admin-b', 'S3curePass!');
  const secondPayload = asRecord(await secondResponse.json());
  expect(secondResponse.status).toBe(409);
  expect(secondPayload).toEqual({
    success: false,
    error: 'Bootstrap already completed',
  });
  expect(state.users).toHaveLength(1);
}, 20_000);

test('login fails on fresh database before bootstrap', async (): Promise<void> => {
  const state: TestState = { users: [], roles: [], orgs: [], tasks: [], audits: [] };
  const db = createMockDb(state);
  const app = buildApp(db);

  const loginResponse = await login(app, 'missing-admin', 'S3curePass!');
  const loginPayload = asRecord(await loginResponse.json());
  expect(loginResponse.status).toBe(401);
  expect(loginPayload).toEqual({
    success: false,
    error: 'Invalid credentials',
  });
}, 20_000);

test('login rejects wrong password after bootstrap', async (): Promise<void> => {
  const state: TestState = { users: [], roles: [], orgs: [], tasks: [], audits: [] };
  const db = createMockDb(state);
  const app = buildApp(db);

  const bootstrapResponse = await bootstrap(app, 'admin', 'S3curePass!');
  expect(bootstrapResponse.status).toBe(201);

  const loginResponse = await login(app, 'admin', 'WrongPass!');
  const loginPayload = asRecord(await loginResponse.json());
  expect(loginResponse.status).toBe(401);
  expect(loginPayload).toEqual({
    success: false,
    error: 'Invalid credentials',
  });
}, 20_000);

test('protected task endpoint rejects missing authorization header', async (): Promise<void> => {
  const state: TestState = { users: [], roles: [], orgs: [], tasks: [], audits: [] };
  const db = createMockDb(state);
  const app = buildApp(db);

  const response = await createProtectedTask(app);
  const payload = asRecord(await response.json());
  expect(response.status).toBe(401);
  expect(payload.success).toBe(false);
  expect(payload.error).toBe('UNAUTHORIZED');
  expect(state.tasks).toHaveLength(0);
}, 20_000);

test('protected task endpoint rejects invalid call depth header', async (): Promise<void> => {
  const state: TestState = { users: [], roles: [], orgs: [], tasks: [], audits: [] };
  const db = createMockDb(state);
  const app = buildApp(db);

  const bootstrapResponse = await bootstrap(app, 'admin', 'S3curePass!');
  expect(bootstrapResponse.status).toBe(201);

  const loginResponse = await login(app, 'admin', 'S3curePass!');
  const loginPayload = asRecord(await loginResponse.json());
  expect(loginResponse.status).toBe(200);

  const taskResponse = await createProtectedTask(app, loginPayload.access_token as string, 'invalid-depth');
  const taskPayload = asRecord(await taskResponse.json());

  expect(taskResponse.status).toBe(400);
  expect(taskPayload).toEqual({
    success: false,
    error: 'INVALID_CALL_DEPTH',
  });
  expect(state.tasks).toHaveLength(0);
}, 20_000);

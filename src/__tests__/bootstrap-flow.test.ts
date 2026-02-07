import { afterEach, beforeEach, expect, test } from 'bun:test';
import { Elysia } from 'elysia';
import type { Collection, Db, Document } from 'mongodb';

import type { TaskDocument, UserDocument } from '../db/collections';
import { authRoute } from '../routes/auth';
import { bootstrapRoute } from '../routes/bootstrap';
import { tasksRoute } from '../routes/tasks';
import { AUDIT_COLLECTION, resetAuditState, type AuditLog } from '../services/audit';

type TestState = {
  users: UserDocument[];
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
        return null;
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

  const db = {
    collection: <TSchema extends Document>(name: string): Collection<TSchema> => {
      if (name === USERS_COLLECTION) {
        return usersCollection as unknown as Collection<TSchema>;
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

const buildApp = (): Elysia => {
  const app = new Elysia();
  bootstrapRoute(app);
  authRoute(app);
  tasksRoute(app);
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

const createProtectedTask = async (app: Elysia, accessToken?: string): Promise<Response> => {
  const headers: HeadersInit = {
    'content-type': 'application/json',
  };
  if (accessToken) {
    headers.authorization = `Bearer ${accessToken}`;
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

const originalJwtSecret = process.env.JWT_SECRET;
const originalMeristemJwtSecret = process.env.MERISTEM_SECURITY_JWT_SECRET;

beforeEach((): void => {
  resetAuditState();
  process.env.MERISTEM_SECURITY_JWT_SECRET = JWT_SECRET_VALUE;
  delete process.env.JWT_SECRET;
});

afterEach((): void => {
  delete (global as { db?: Db }).db;
  if (originalJwtSecret === undefined) {
    delete process.env.JWT_SECRET;
  } else {
    process.env.JWT_SECRET = originalJwtSecret;
  }

  if (originalMeristemJwtSecret === undefined) {
    delete process.env.MERISTEM_SECURITY_JWT_SECRET;
  } else {
    process.env.MERISTEM_SECURITY_JWT_SECRET = originalMeristemJwtSecret;
  }
});

test('fresh db bootstrap -> login -> protected task creation succeeds', async (): Promise<void> => {
  const state: TestState = { users: [], tasks: [], audits: [] };
  (global as { db?: Db }).db = createMockDb(state);
  const app = buildApp();

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
});

test('second bootstrap attempt is rejected after first user exists', async (): Promise<void> => {
  const state: TestState = { users: [], tasks: [], audits: [] };
  (global as { db?: Db }).db = createMockDb(state);
  const app = buildApp();

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
});

test('login fails on fresh database before bootstrap', async (): Promise<void> => {
  const state: TestState = { users: [], tasks: [], audits: [] };
  (global as { db?: Db }).db = createMockDb(state);
  const app = buildApp();

  const loginResponse = await login(app, 'missing-admin', 'S3curePass!');
  const loginPayload = asRecord(await loginResponse.json());
  expect(loginResponse.status).toBe(401);
  expect(loginPayload).toEqual({
    success: false,
    error: 'Invalid credentials',
  });
});

test('login rejects wrong password after bootstrap', async (): Promise<void> => {
  const state: TestState = { users: [], tasks: [], audits: [] };
  (global as { db?: Db }).db = createMockDb(state);
  const app = buildApp();

  const bootstrapResponse = await bootstrap(app, 'admin', 'S3curePass!');
  expect(bootstrapResponse.status).toBe(201);

  const loginResponse = await login(app, 'admin', 'WrongPass!');
  const loginPayload = asRecord(await loginResponse.json());
  expect(loginResponse.status).toBe(401);
  expect(loginPayload).toEqual({
    success: false,
    error: 'Invalid credentials',
  });
});

test('protected task endpoint rejects missing authorization header', async (): Promise<void> => {
  const state: TestState = { users: [], tasks: [], audits: [] };
  (global as { db?: Db }).db = createMockDb(state);
  const app = buildApp();

  const response = await createProtectedTask(app);
  const payload = asRecord(await response.json());
  expect(response.status).toBe(401);
  expect(payload.success).toBe(false);
  expect(payload.error).toBe('UNAUTHORIZED');
  expect(state.tasks).toHaveLength(0);
});

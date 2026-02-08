import { afterEach, beforeEach, expect, test } from 'bun:test';
import { Elysia } from 'elysia';
import type { Db } from 'mongodb';

import type { UserDocument } from '../db/collections';
import { DEFAULT_ORG_ID, SUPERADMIN_ROLE_ID } from '../services/bootstrap';
import { usersRoute } from '../routes/users';
import { createBearerToken } from './phase2-auth-helper';
import { createInMemoryDb, createInMemoryDbState } from './phase2-test-helper';

const JWT_SECRET_VALUE = 'phase2-users-secret';

const originalJwtSignSecret = process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;

const asRecord = (value: unknown): Record<string, unknown> => value as Record<string, unknown>;

const createUserDoc = (overrides: Partial<UserDocument> = {}): UserDocument => {
  const now = new Date('2026-02-08T00:00:00.000Z');
  return {
    user_id: 'u-superadmin',
    username: 'superadmin',
    password_hash: 'hash',
    role_ids: [SUPERADMIN_ROLE_ID],
    org_id: DEFAULT_ORG_ID,
    permissions: ['*'],
    permissions_v: 1,
    tokens: [],
    created_at: now,
    updated_at: now,
    ...overrides,
  };
};

beforeEach((): void => {
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = JWT_SECRET_VALUE;
});

afterEach((): void => {
  delete (global as { db?: Db }).db;
  if (originalJwtSignSecret === undefined) {
    delete process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;
  } else {
    process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = originalJwtSignSecret;
  }
});

test('users API enforces superadmin and supports CRUD', async (): Promise<void> => {
  const state = createInMemoryDbState();
  state.users.push(createUserDoc());
  (global as { db?: Db }).db = createInMemoryDb(state);

  const app = new Elysia();
  usersRoute(app);

  const normalAuth = await createBearerToken(
    {
      sub: 'u-normal',
      type: 'USER',
      permissions: ['node:read'],
    },
    JWT_SECRET_VALUE,
  );

  const deniedListResp = await app.handle(
    new Request('http://localhost/api/v1/users', {
      method: 'GET',
      headers: { authorization: normalAuth },
    }),
  );
  expect(deniedListResp.status).toBe(403);

  const superadminAuth = await createBearerToken(
    {
      sub: 'u-superadmin',
      type: 'USER',
      permissions: ['*'],
    },
    JWT_SECRET_VALUE,
  );

  const createResp = await app.handle(
    new Request('http://localhost/api/v1/users', {
      method: 'POST',
      headers: {
        authorization: superadminAuth,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        username: 'new-user',
        password: 'S3curePass!',
        org_id: 'org-alpha',
      }),
    }),
  );
  expect(createResp.status).toBe(201);
  const createPayload = asRecord(await createResp.json());
  expect(createPayload.success).toBe(true);
  expect(typeof createPayload.user_id).toBe('string');

  const listResp = await app.handle(
    new Request('http://localhost/api/v1/users?limit=50&offset=0', {
      method: 'GET',
      headers: { authorization: superadminAuth },
    }),
  );
  expect(listResp.status).toBe(200);
  const listPayload = asRecord(await listResp.json());
  expect(Array.isArray(listPayload.data)).toBe(true);
  expect((listPayload.data as unknown[]).length).toBe(2);

  const createdUserId = createPayload.user_id as string;
  const getResp = await app.handle(
    new Request(`http://localhost/api/v1/users/${createdUserId}`, {
      method: 'GET',
      headers: { authorization: superadminAuth },
    }),
  );
  expect(getResp.status).toBe(200);

  const patchResp = await app.handle(
    new Request(`http://localhost/api/v1/users/${createdUserId}`, {
      method: 'PATCH',
      headers: {
        authorization: superadminAuth,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        username: 'new-user-renamed',
      }),
    }),
  );
  expect(patchResp.status).toBe(200);
  const patchPayload = asRecord(await patchResp.json());
  const patchedUser = asRecord(patchPayload.data);
  expect(patchedUser.username).toBe('new-user-renamed');

  const deleteResp = await app.handle(
    new Request(`http://localhost/api/v1/users/${createdUserId}`, {
      method: 'DELETE',
      headers: { authorization: superadminAuth },
    }),
  );
  expect(deleteResp.status).toBe(200);

  const listAfterDeleteResp = await app.handle(
    new Request('http://localhost/api/v1/users?limit=50&offset=0', {
      method: 'GET',
      headers: { authorization: superadminAuth },
    }),
  );
  const listAfterDeletePayload = asRecord(await listAfterDeleteResp.json());
  expect(Array.isArray(listAfterDeletePayload.data)).toBe(true);
  expect((listAfterDeletePayload.data as unknown[]).length).toBe(1);
}, 20_000);

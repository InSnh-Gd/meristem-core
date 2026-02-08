import { afterEach, beforeEach, expect, test } from 'bun:test';
import { Elysia } from 'elysia';

import type { RoleDocument, UserDocument } from '../db/collections';
import { DEFAULT_ORG_ID, SUPERADMIN_ROLE_ID, SUPERADMIN_ROLE_NAME } from '../services/bootstrap';
import { rolesRoute } from '../routes/roles';
import { usersRoute } from '../routes/users';
import { createBearerToken } from './phase2-auth-helper';
import { createInMemoryDb, createInMemoryDbState } from './phase2-test-helper';

const JWT_SECRET_VALUE = 'phase2-roles-secret';

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

const createRoleDoc = (overrides: Partial<RoleDocument> = {}): RoleDocument => {
  const now = new Date('2026-02-08T00:00:00.000Z');
  return {
    role_id: SUPERADMIN_ROLE_ID,
    name: SUPERADMIN_ROLE_NAME,
    description: 'Built-in superadmin role',
    permissions: ['*'],
    is_builtin: true,
    org_id: DEFAULT_ORG_ID,
    created_at: now,
    updated_at: now,
    ...overrides,
  };
};

beforeEach((): void => {
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = JWT_SECRET_VALUE;
});

afterEach((): void => {
  if (originalJwtSignSecret === undefined) {
    delete process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;
  } else {
    process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = originalJwtSignSecret;
  }
});

test('role CRUD and role assignment sync user.permissions', async (): Promise<void> => {
  const state = createInMemoryDbState();
  state.users.push(createUserDoc());
  state.users.push(
    createUserDoc({
      user_id: 'u-target',
      username: 'target-user',
      role_ids: [],
      permissions: [],
      org_id: 'org-alpha',
    }),
  );
  state.roles.push(createRoleDoc());
  const db = createInMemoryDb(state);

  const app = new Elysia();
  usersRoute(app, db);
  rolesRoute(app, db);

  const superadminAuth = await createBearerToken(
    {
      sub: 'u-superadmin',
      type: 'USER',
      permissions: ['*'],
    },
    JWT_SECRET_VALUE,
  );

  const createRoleResp = await app.handle(
    new Request('http://localhost/api/v1/roles', {
      method: 'POST',
      headers: {
        authorization: superadminAuth,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        name: 'operator',
        description: 'operator role',
        permissions: ['tasks:create', 'node:read'],
        org_id: 'org-alpha',
      }),
    }),
  );
  expect(createRoleResp.status).toBe(201);
  const createRolePayload = asRecord(await createRoleResp.json());
  const roleId = createRolePayload.role_id as string;
  expect(typeof roleId).toBe('string');

  const assignRoleResp = await app.handle(
    new Request('http://localhost/api/v1/users/u-target/roles', {
      method: 'POST',
      headers: {
        authorization: superadminAuth,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        role_id: roleId,
      }),
    }),
  );
  expect(assignRoleResp.status).toBe(200);

  const assignedUser = state.users.find((user) => user.user_id === 'u-target');
  expect(assignedUser?.role_ids.includes(roleId)).toBe(true);
  expect(assignedUser?.permissions.includes('tasks:create')).toBe(true);
  expect(assignedUser?.permissions.includes('node:read')).toBe(true);

  const removeRoleResp = await app.handle(
    new Request(`http://localhost/api/v1/users/u-target/roles/${roleId}`, {
      method: 'DELETE',
      headers: {
        authorization: superadminAuth,
      },
    }),
  );
  expect(removeRoleResp.status).toBe(200);

  const userAfterRemoval = state.users.find((user) => user.user_id === 'u-target');
  expect(userAfterRemoval?.role_ids.includes(roleId)).toBe(false);
  expect(userAfterRemoval?.permissions.includes('tasks:create')).toBe(false);

  const deleteBuiltinRoleResp = await app.handle(
    new Request(`http://localhost/api/v1/roles/${SUPERADMIN_ROLE_ID}`, {
      method: 'DELETE',
      headers: {
        authorization: superadminAuth,
      },
    }),
  );
  expect(deleteBuiltinRoleResp.status).toBe(400);
});

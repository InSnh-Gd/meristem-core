import { afterEach, beforeEach, expect, test } from 'bun:test';
import { Elysia } from 'elysia';
import type { Db } from 'mongodb';

import type { RoleDocument, UserDocument } from '../db/collections';
import { DEFAULT_ORG_ID, SUPERADMIN_ROLE_ID } from '../services/bootstrap';
import { usersRoute } from '../routes/users';
import { createBearerToken } from './phase2-auth-helper';
import { createInMemoryDb, createInMemoryDbState } from './phase2-test-helper';

const JWT_SECRET_VALUE = 'phase2-invitations-secret';

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
    role_id: 'role-operator',
    name: 'operator',
    description: 'operator role',
    permissions: ['tasks:create'],
    is_builtin: false,
    org_id: 'org-alpha',
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

test('invitation flow creates invitation and accepts it to activate user', async (): Promise<void> => {
  const state = createInMemoryDbState();
  state.users.push(createUserDoc());
  state.roles.push(createRoleDoc());
  (global as { db?: Db }).db = createInMemoryDb(state);

  const app = new Elysia();
  usersRoute(app);

  const superadminAuth = await createBearerToken(
    {
      sub: 'u-superadmin',
      type: 'USER',
      permissions: ['*'],
    },
    JWT_SECRET_VALUE,
  );

  const createInvitationResp = await app.handle(
    new Request('http://localhost/api/v1/users/invitations', {
      method: 'POST',
      headers: {
        authorization: superadminAuth,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        username: 'invited-user',
        org_id: 'org-alpha',
        role_ids: ['role-operator'],
        expires_in_hours: 24,
      }),
    }),
  );
  expect(createInvitationResp.status).toBe(201);
  const invitationPayload = asRecord(await createInvitationResp.json());
  const invitationToken = invitationPayload.invitation_token as string;
  expect(typeof invitationToken).toBe('string');

  const acceptInvitationResp = await app.handle(
    new Request('http://localhost/api/v1/users/invitations/accept', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        invitation_token: invitationToken,
        password: 'S3curePass!',
      }),
    }),
  );
  expect(acceptInvitationResp.status).toBe(201);
  const acceptPayload = asRecord(await acceptInvitationResp.json());
  expect(acceptPayload.success).toBe(true);
  expect(typeof acceptPayload.user_id).toBe('string');

  const createdUser = state.users.find((user) => user.username === 'invited-user');
  expect(createdUser).toBeDefined();
  expect(createdUser?.org_id).toBe('org-alpha');
  expect(createdUser?.role_ids).toEqual(['role-operator']);
  expect(createdUser?.permissions.includes('tasks:create')).toBe(true);

  const secondAcceptResp = await app.handle(
    new Request('http://localhost/api/v1/users/invitations/accept', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        invitation_token: invitationToken,
        password: 'S3curePass!',
      }),
    }),
  );
  expect(secondAcceptResp.status).toBe(409);
}, 20_000);

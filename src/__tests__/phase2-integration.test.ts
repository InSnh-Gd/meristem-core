import { afterEach, beforeEach, expect, test } from 'bun:test';
import { Elysia } from 'elysia';

import { authRoute } from '../routes/auth';
import { auditRoute } from '../routes/audit';
import { bootstrapRoute } from '../routes/bootstrap';
import { joinRoute } from '../routes/join';
import { rolesRoute } from '../routes/roles';
import { tasksRoute } from '../routes/tasks';
import { usersRoute } from '../routes/users';
import { createInMemoryDb, createInMemoryDbState } from './phase2-test-helper';

const JWT_SECRET_VALUE = 'phase2-integration-secret';

const originalJwtSignSecret = process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;

const asRecord = (value: unknown): Record<string, unknown> => value as Record<string, unknown>;

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

const createRole = async (
  app: Elysia,
  auth: string,
  payload: Record<string, unknown>,
): Promise<Response> =>
  app.handle(
    new Request('http://localhost/api/v1/roles', {
      method: 'POST',
      headers: {
        authorization: auth,
        'content-type': 'application/json',
      },
      body: JSON.stringify(payload),
    }),
  );

const createInvitation = async (
  app: Elysia,
  auth: string,
  payload: Record<string, unknown>,
): Promise<Response> =>
  app.handle(
    new Request('http://localhost/api/v1/users/invitations', {
      method: 'POST',
      headers: {
        authorization: auth,
        'content-type': 'application/json',
      },
      body: JSON.stringify(payload),
    }),
  );

const acceptInvitation = async (
  app: Elysia,
  invitationToken: string,
  password: string,
): Promise<Response> =>
  app.handle(
    new Request('http://localhost/api/v1/users/invitations/accept', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        invitation_token: invitationToken,
        password,
      }),
    }),
  );

const createTask = async (app: Elysia, auth: string, name: string): Promise<Response> =>
  app.handle(
    new Request('http://localhost/api/v1/tasks', {
      method: 'POST',
      headers: {
        authorization: auth,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        name,
        payload: {
          action: 'noop',
        },
      }),
    }),
  );

const listTasks = async (app: Elysia, auth: string): Promise<Response> =>
  app.handle(
    new Request('http://localhost/api/v1/tasks?limit=100&offset=0', {
      method: 'GET',
      headers: {
        authorization: auth,
      },
    }),
  );

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

test('phase2 chain: bootstrap -> invitation -> role sync -> permission check -> org isolation', async (): Promise<void> => {
  const state = createInMemoryDbState();
  const db = createInMemoryDb(state);

  const app = new Elysia();
  bootstrapRoute(app, db);
  authRoute(app, db);
  usersRoute(app, db);
  rolesRoute(app, db);
  tasksRoute(app, db);
  auditRoute(app, db);
  joinRoute(app, db);

  const bootstrapResp = await bootstrap(app, 'admin', 'S3curePass!');
  expect(bootstrapResp.status).toBe(201);
  expect(state.orgs.length).toBeGreaterThan(0);
  expect(state.roles.length).toBeGreaterThan(0);

  const adminLoginResp = await login(app, 'admin', 'S3curePass!');
  expect(adminLoginResp.status).toBe(200);
  const adminLoginPayload = asRecord(await adminLoginResp.json());
  const adminAccessToken = adminLoginPayload.access_token as string;
  const adminAuth = `Bearer ${adminAccessToken}`;

  const roleAlphaResp = await createRole(app, adminAuth, {
    name: 'operator-alpha',
    description: 'operator alpha role',
    permissions: ['tasks:create'],
    org_id: 'org-alpha',
  });
  expect(roleAlphaResp.status).toBe(201);
  const roleAlphaPayload = asRecord(await roleAlphaResp.json());
  const roleAlphaId = roleAlphaPayload.role_id as string;

  const roleBetaResp = await createRole(app, adminAuth, {
    name: 'operator-beta',
    description: 'operator beta role',
    permissions: ['tasks:create'],
    org_id: 'org-beta',
  });
  expect(roleBetaResp.status).toBe(201);
  const roleBetaPayload = asRecord(await roleBetaResp.json());
  const roleBetaId = roleBetaPayload.role_id as string;

  const invitationAlphaResp = await createInvitation(app, adminAuth, {
    username: 'alpha-user',
    org_id: 'org-alpha',
    role_ids: [roleAlphaId],
  });
  expect(invitationAlphaResp.status).toBe(201);
  const invitationAlphaPayload = asRecord(await invitationAlphaResp.json());
  const invitationAlphaToken = invitationAlphaPayload.invitation_token as string;

  const acceptAlphaResp = await acceptInvitation(app, invitationAlphaToken, 'S3curePass!');
  expect(acceptAlphaResp.status).toBe(201);

  const invitationBetaResp = await createInvitation(app, adminAuth, {
    username: 'beta-user',
    org_id: 'org-beta',
    role_ids: [roleBetaId],
  });
  expect(invitationBetaResp.status).toBe(201);
  const invitationBetaPayload = asRecord(await invitationBetaResp.json());
  const invitationBetaToken = invitationBetaPayload.invitation_token as string;

  const acceptBetaResp = await acceptInvitation(app, invitationBetaToken, 'S3curePass!');
  expect(acceptBetaResp.status).toBe(201);

  const alphaLoginResp = await login(app, 'alpha-user', 'S3curePass!');
  expect(alphaLoginResp.status).toBe(200);
  const alphaLoginPayload = asRecord(await alphaLoginResp.json());
  const alphaAuth = `Bearer ${alphaLoginPayload.access_token as string}`;

  const betaLoginResp = await login(app, 'beta-user', 'S3curePass!');
  expect(betaLoginResp.status).toBe(200);
  const betaLoginPayload = asRecord(await betaLoginResp.json());
  const betaAuth = `Bearer ${betaLoginPayload.access_token as string}`;

  const alphaTaskResp = await createTask(app, alphaAuth, 'alpha-task');
  expect(alphaTaskResp.status).toBe(201);
  const betaTaskResp = await createTask(app, betaAuth, 'beta-task');
  expect(betaTaskResp.status).toBe(201);

  const alphaTasksResp = await listTasks(app, alphaAuth);
  expect(alphaTasksResp.status).toBe(200);
  const alphaTasksPayload = asRecord(await alphaTasksResp.json());
  expect(Array.isArray(alphaTasksPayload.data)).toBe(true);
  expect((alphaTasksPayload.data as unknown[]).length).toBe(1);

  const adminTasksResp = await listTasks(app, adminAuth);
  expect(adminTasksResp.status).toBe(200);
  const adminTasksPayload = asRecord(await adminTasksResp.json());
  expect(Array.isArray(adminTasksPayload.data)).toBe(true);
  expect((adminTasksPayload.data as unknown[]).length).toBe(2);

  const betaAuditResp = await app.handle(
    new Request('http://localhost/api/v1/audit-logs', {
      method: 'GET',
      headers: {
        authorization: betaAuth,
      },
    }),
  );
  expect(betaAuditResp.status).toBe(403);

  const joinResp = await app.handle(
    new Request('http://localhost/api/v1/join', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        hwid: 'e'.repeat(64),
        hostname: 'phase2-node',
        persona: 'AGENT',
        org_id: 'org-alpha',
      }),
    }),
  );
  expect(joinResp.status).toBe(200);
  expect(state.nodes).toHaveLength(1);
  expect(state.nodes[0]?.org_id).toBe('org-alpha');
}, 40_000);

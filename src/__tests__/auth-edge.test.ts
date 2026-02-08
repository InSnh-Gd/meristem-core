import { afterEach, beforeEach, expect, test } from 'bun:test';
import { SignJWT } from 'jose';

import { requireAuth } from '../middleware/auth';
import { createTraceContext } from '../utils/trace-context';

const secretBytes = (value: string): Uint8Array => new TextEncoder().encode(value);

const signToken = async (
  payload: Record<string, unknown>,
  secret: string,
): Promise<string> => {
  return new SignJWT(payload)
    .setProtectedHeader({ alg: 'HS256' })
    .sign(secretBytes(secret));
};

type RequireAuthContext = {
  headers: { authorization?: string };
  set: { status?: unknown };
  store: Record<string, unknown>;
  traceContext: ReturnType<typeof createTraceContext>;
};

const createAuthContext = (authorization?: string): RequireAuthContext => ({
  headers: authorization ? { authorization } : {},
  set: {},
  store: {},
  traceContext: createTraceContext({
    nodeId: 'core',
    source: 'auth-edge-test',
    traceId: 'trace-auth-edge',
  }),
});

const createAuthContextWithoutTrace = (authorization: string): Omit<RequireAuthContext, 'traceContext'> => ({
  headers: { authorization },
  set: {},
  store: {},
});

const originalJwtSignSecret = process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;
const originalMeristemJwtVerifySecrets = process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS;
const originalLegacyJwtSecret = process.env.MERISTEM_SECURITY_JWT_SECRET;

beforeEach((): void => {
  delete process.env.MERISTEM_SECURITY_JWT_SECRET;
  delete process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;
  delete process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS;
});

afterEach((): void => {
  if (originalLegacyJwtSecret === undefined) {
    delete process.env.MERISTEM_SECURITY_JWT_SECRET;
  } else {
    process.env.MERISTEM_SECURITY_JWT_SECRET = originalLegacyJwtSecret;
  }

  if (originalJwtSignSecret === undefined) {
    delete process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;
  } else {
    process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = originalJwtSignSecret;
  }

  if (originalMeristemJwtVerifySecrets === undefined) {
    delete process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS;
  } else {
    process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS = originalMeristemJwtVerifySecrets;
  }
});

test('requireAuth rejects missing authorization header', async (): Promise<void> => {
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'auth-edge-secret';
  const context = createAuthContext();
  const response = await requireAuth(context);

  expect(context.set.status).toBe(401);
  expect(response).toEqual({
    success: false,
    error: 'UNAUTHORIZED',
  });
});

test('requireAuth rejects malformed bearer scheme', async (): Promise<void> => {
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'auth-edge-secret';
  const context = createAuthContext('bearer token-value');
  const response = await requireAuth(context);

  expect(context.set.status).toBe(401);
  expect(response).toEqual({
    success: false,
    error: 'UNAUTHORIZED',
  });
});

test('requireAuth rejects expired jwt token', async (): Promise<void> => {
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'auth-edge-secret';
  const now = Math.floor(Date.now() / 1000);

  const token = await signToken(
    {
      sub: 'user-expired',
      type: 'USER',
      permissions: ['tasks:create'],
      exp: now - 5,
    },
    'auth-edge-secret',
  );

  const context = createAuthContext(`Bearer ${token}`);
  const response = await requireAuth(context);

  expect(context.set.status).toBe(401);
  expect(response).toEqual({
    success: false,
    error: 'UNAUTHORIZED',
  });
});

test('requireAuth rejects jwt token with invalid payload shape', async (): Promise<void> => {
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'auth-edge-secret';
  const now = Math.floor(Date.now() / 1000);

  const token = await signToken(
    {
      sub: 'user-invalid',
      type: 'USER',
      permissions: 'tasks:create',
      exp: now + 60,
    },
    'auth-edge-secret',
  );

  const context = createAuthContext(`Bearer ${token}`);
  const response = await requireAuth(context);

  expect(context.set.status).toBe(401);
  expect(response).toEqual({
    success: false,
    error: 'UNAUTHORIZED',
  });
});

test('requireAuth accepts valid token when only MERISTEM_SECURITY_JWT_SIGN_SECRET is configured', async (): Promise<void> => {
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'meristem-only-secret';
  const now = Math.floor(Date.now() / 1000);

  const token = await signToken(
    {
      sub: 'user-meristem',
      type: 'USER',
      permissions: ['tasks:create'],
      exp: now + 120,
    },
    'meristem-only-secret',
  );

  const context = createAuthContext(`Bearer ${token}`);
  const response = await requireAuth(context);

  expect(response).toBeUndefined();
  expect(context.set.status).toBeUndefined();
  expect(context.store.user).toEqual({
    id: 'user-meristem',
    type: 'USER',
    permissions: ['tasks:create'],
    node_id: undefined,
  });
});

test('requireAuth rejects token when only legacy secret env is set', async (): Promise<void> => {
  process.env.MERISTEM_SECURITY_JWT_SECRET = 'legacy-only-secret';
  const now = Math.floor(Date.now() / 1000);

  const token = await signToken(
    {
      sub: 'user-legacy-only',
      type: 'USER',
      permissions: ['tasks:create'],
      exp: now + 120,
    },
    'legacy-only-secret',
  );

  const context = createAuthContext(`Bearer ${token}`);
  const response = await requireAuth(context);

  expect(context.set.status).toBe(401);
  expect(response).toEqual({
    success: false,
    error: 'UNAUTHORIZED',
  });
});

test('requireAuth accepts valid token even when traceContext is missing', async (): Promise<void> => {
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'auth-edge-secret';
  const now = Math.floor(Date.now() / 1000);

  const token = await signToken(
    {
      sub: 'user-no-trace',
      type: 'USER',
      permissions: ['tasks:create'],
      exp: now + 120,
    },
    'auth-edge-secret',
  );

  const context = createAuthContextWithoutTrace(`Bearer ${token}`);
  const response = await requireAuth(context as unknown as Parameters<typeof requireAuth>[0]);

  expect(response).toBeUndefined();
  expect(context.set.status).toBeUndefined();
  expect(context.store.user).toEqual({
    id: 'user-no-trace',
    type: 'USER',
    permissions: ['tasks:create'],
    node_id: undefined,
  });
});

test('requireAuth rejects invalid token without traceContext instead of throwing', async (): Promise<void> => {
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'auth-edge-secret';
  const context = createAuthContextWithoutTrace('Bearer invalid-token');

  const response = await requireAuth(context as unknown as Parameters<typeof requireAuth>[0]);

  expect(context.set.status).toBe(401);
  expect(response).toEqual({
    success: false,
    error: 'UNAUTHORIZED',
  });
});

test('requireAuth accepts token signed by old secret when verify list contains old and new secret', async (): Promise<void> => {
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'new-sign-secret';
  process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS = 'new-sign-secret,old-verify-secret';
  const now = Math.floor(Date.now() / 1000);

  const token = await signToken(
    {
      sub: 'user-rotated',
      type: 'USER',
      permissions: ['tasks:create'],
      exp: now + 120,
    },
    'old-verify-secret',
  );

  const context = createAuthContext(`Bearer ${token}`);
  const response = await requireAuth(context);

  expect(response).toBeUndefined();
  expect(context.set.status).toBeUndefined();
  expect(context.store.user).toEqual({
    id: 'user-rotated',
    type: 'USER',
    permissions: ['tasks:create'],
    node_id: undefined,
  });
});

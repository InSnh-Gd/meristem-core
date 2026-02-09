import { afterEach, beforeEach, expect, test } from 'bun:test';
import { Elysia } from 'elysia';

import { metricsRoute } from '../routes/metrics';
import { createBearerToken } from './phase2-auth-helper';

const JWT_SECRET_VALUE = 'phase2-metrics-secret';
const originalJwtSignSecret = process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;

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

test('metrics endpoint requires auth and returns prometheus payload for superadmin', async (): Promise<void> => {
  const app = new Elysia();
  metricsRoute(app);

  const unauthorizedResponse = await app.handle(
    new Request('http://localhost/metrics', {
      method: 'GET',
    }),
  );
  expect(unauthorizedResponse.status).toBe(401);

  const auth = await createBearerToken(
    {
      sub: 'u-superadmin',
      type: 'USER',
      permissions: ['*'],
    },
    JWT_SECRET_VALUE,
  );

  const response = await app.handle(
    new Request('http://localhost/metrics', {
      method: 'GET',
      headers: {
        authorization: auth,
      },
    }),
  );
  expect(response.status).toBe(200);
  const body = await response.text();
  expect(body.includes('meristem_db_queries_total')).toBe(true);
  expect(body.includes('meristem_db_transactions_total')).toBe(true);
});

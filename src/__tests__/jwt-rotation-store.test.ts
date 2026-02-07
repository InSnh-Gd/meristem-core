import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';
import { loadConfig } from '../config';
import {
  readJwtRotationState,
  readJwtRotationStateSync,
  writeJwtRotationState,
  type JwtRotationState,
} from '../config/jwt-rotation-store';

const originalEnv = {
  MERISTEM_SECURITY_JWT_SIGN_SECRET: process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET,
  MERISTEM_SECURITY_JWT_VERIFY_SECRETS: process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS,
  MERISTEM_SECURITY_JWT_SECRET: process.env.MERISTEM_SECURITY_JWT_SECRET,
  MERISTEM_SECURITY_JWT_ROTATION_STORE_PATH: process.env.MERISTEM_SECURITY_JWT_ROTATION_STORE_PATH,
};

const tempRoots: string[] = [];

const restoreEnv = (): void => {
  for (const [key, value] of Object.entries(originalEnv)) {
    if (value === undefined) {
      delete process.env[key];
      continue;
    }
    process.env[key] = value;
  }
};

afterEach(async (): Promise<void> => {
  restoreEnv();
  for (const root of tempRoots.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

const createTempStorePath = async (): Promise<string> => {
  const root = await mkdtemp(join(tmpdir(), 'meristem-jwt-rotation-'));
  tempRoots.push(root);
  return join(root, 'jwt-rotation.json');
};

test('write/read jwt rotation state roundtrip', async (): Promise<void> => {
  const storePath = await createTempStorePath();
  const inputState: JwtRotationState = {
    current_sign_secret: 'sign-secret-new',
    verify_secrets: ['sign-secret-new', 'sign-secret-old'],
    rotated_at: '2026-02-07T12:00:00.000Z',
    grace_seconds: 3600,
  };

  await writeJwtRotationState(storePath, inputState);
  const loaded = await readJwtRotationState(storePath);

  expect(loaded).toEqual(inputState);
});

test('readJwtRotationStateSync returns normalized state for existing store file', async (): Promise<void> => {
  const storePath = await createTempStorePath();
  const inputState: JwtRotationState = {
    current_sign_secret: 'sign-secret-new',
    verify_secrets: ['sign-secret-new', 'sign-secret-old'],
    rotated_at: '2026-02-07T12:00:00.000Z',
    grace_seconds: 3600,
  };

  await writeJwtRotationState(storePath, inputState);
  const loaded = readJwtRotationStateSync(storePath);

  expect(loaded).toEqual(inputState);
});

test('loadConfig uses rotation store secret when env sign secret is missing', async (): Promise<void> => {
  const storePath = await createTempStorePath();
  await writeJwtRotationState(storePath, {
    current_sign_secret: 'rotation-sign-secret',
    verify_secrets: ['rotation-sign-secret', 'rotation-old-secret'],
    rotated_at: '2026-02-07T12:00:00.000Z',
    grace_seconds: 7200,
  });

  delete process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;
  delete process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS;
  delete process.env.MERISTEM_SECURITY_JWT_SECRET;
  process.env.MERISTEM_SECURITY_JWT_ROTATION_STORE_PATH = storePath;

  const config = loadConfig();

  expect(config.security.jwt_sign_secret).toBe('rotation-sign-secret');
  expect(config.security.jwt_verify_secrets).toEqual(['rotation-sign-secret', 'rotation-old-secret']);
  expect(config.security.jwt_rotation_grace_seconds).toBe(7200);
});

test('loadConfig prefers env sign secret over rotation store secret', async (): Promise<void> => {
  const storePath = await createTempStorePath();
  await writeJwtRotationState(storePath, {
    current_sign_secret: 'rotation-sign-secret',
    verify_secrets: ['rotation-sign-secret', 'rotation-old-secret'],
    rotated_at: '2026-02-07T12:00:00.000Z',
    grace_seconds: 7200,
  });

  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'env-sign-secret';
  process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS = 'env-sign-secret,env-legacy-secret';
  process.env.MERISTEM_SECURITY_JWT_ROTATION_STORE_PATH = storePath;

  const config = loadConfig();

  expect(config.security.jwt_sign_secret).toBe('env-sign-secret');
  expect(config.security.jwt_verify_secrets).toEqual(['env-sign-secret', 'env-legacy-secret']);
  expect(config.security.jwt_rotation_grace_seconds).toBe(7200);
});

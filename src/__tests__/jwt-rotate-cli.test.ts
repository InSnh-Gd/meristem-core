import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';
import { readJwtRotationState } from '../config/jwt-rotation-store';
import { runJwtRotationCli } from '../cli/jwt-rotate';

const originalEnv = {
  MERISTEM_SECURITY_JWT_SIGN_SECRET: process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET,
  MERISTEM_SECURITY_JWT_VERIFY_SECRETS: process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS,
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
  const root = await mkdtemp(join(tmpdir(), 'meristem-jwt-cli-'));
  tempRoots.push(root);
  return join(root, 'jwt-rotation.json');
};

test('jwt rotate command writes new sign secret and preserves old verify secret', async (): Promise<void> => {
  const storePath = await createTempStorePath();

  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'old-sign-secret';
  process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS = 'old-sign-secret,legacy-secret';

  const exitCode = await runJwtRotationCli(['rotate', '--store', storePath, '--grace-seconds', '120'], {
    randomSecret: () => 'new-sign-secret',
    nowMs: () => Date.parse('2026-02-07T12:00:00.000Z'),
  });

  expect(exitCode).toBe(0);

  const state = await readJwtRotationState(storePath);
  expect(state).not.toBeNull();
  expect(state?.current_sign_secret).toBe('new-sign-secret');
  expect(state?.verify_secrets).toEqual(['new-sign-secret', 'old-sign-secret', 'legacy-secret']);
  expect(state?.grace_seconds).toBe(120);
});

test('jwt prune command refuses pruning before grace deadline', async (): Promise<void> => {
  const storePath = await createTempStorePath();

  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'old-sign-secret';
  await runJwtRotationCli(['rotate', '--store', storePath, '--grace-seconds', '3600'], {
    randomSecret: () => 'new-sign-secret',
    nowMs: () => Date.parse('2026-02-07T12:00:00.000Z'),
  });

  const exitCode = await runJwtRotationCli(['prune', '--store', storePath], {
    nowMs: () => Date.parse('2026-02-07T12:30:00.000Z'),
  });

  expect(exitCode).toBe(1);
  const state = await readJwtRotationState(storePath);
  expect(state?.verify_secrets).toEqual(['new-sign-secret', 'old-sign-secret']);
});

test('jwt prune command keeps only current sign secret after grace deadline', async (): Promise<void> => {
  const storePath = await createTempStorePath();

  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'old-sign-secret';
  await runJwtRotationCli(['rotate', '--store', storePath, '--grace-seconds', '60'], {
    randomSecret: () => 'new-sign-secret',
    nowMs: () => Date.parse('2026-02-07T12:00:00.000Z'),
  });

  const exitCode = await runJwtRotationCli(['prune', '--store', storePath], {
    nowMs: () => Date.parse('2026-02-07T12:02:00.000Z'),
  });

  expect(exitCode).toBe(0);
  const state = await readJwtRotationState(storePath);
  expect(state?.verify_secrets).toEqual(['new-sign-secret']);
});

import { mkdir, readFile, writeFile } from 'fs/promises';
import { dirname } from 'path';

export type JwtRotationState = {
  current_sign_secret: string;
  verify_secrets: string[];
  rotated_at: string;
  grace_seconds: number;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isNonEmptyString = (value: unknown): value is string =>
  typeof value === 'string' && value.trim().length > 0;

const normalizeVerifySecrets = (
  currentSignSecret: string,
  verifySecrets: readonly string[],
): string[] => {
  const seen = new Set<string>();
  const normalized: string[] = [];

  const append = (secret: string): void => {
    if (!secret || seen.has(secret)) {
      return;
    }
    seen.add(secret);
    normalized.push(secret);
  };

  append(currentSignSecret);
  for (const secret of verifySecrets) {
    append(secret);
  }

  return normalized;
};

const parseGraceSeconds = (value: unknown): number => {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 1) {
    return 86400;
  }
  return Math.floor(value);
};

const parseRotatedAt = (value: unknown): string => {
  if (!isNonEmptyString(value)) {
    return new Date(0).toISOString();
  }
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return new Date(0).toISOString();
  }
  return new Date(timestamp).toISOString();
};

const normalizeState = (value: unknown): JwtRotationState | null => {
  if (!isRecord(value)) {
    return null;
  }

  const currentSignSecret = value.current_sign_secret;
  if (!isNonEmptyString(currentSignSecret)) {
    return null;
  }

  const verifySecrets = Array.isArray(value.verify_secrets)
    ? value.verify_secrets.filter((item): item is string => isNonEmptyString(item))
    : [];

  return {
    current_sign_secret: currentSignSecret,
    verify_secrets: normalizeVerifySecrets(currentSignSecret, verifySecrets),
    rotated_at: parseRotatedAt(value.rotated_at),
    grace_seconds: parseGraceSeconds(value.grace_seconds),
  };
};

export const readJwtRotationState = async (storePath: string): Promise<JwtRotationState | null> => {
  try {
    const raw = await readFile(storePath, 'utf-8');
    const parsed = JSON.parse(raw) as unknown;
    return normalizeState(parsed);
  } catch {
    return null;
  }
};

export const writeJwtRotationState = async (
  storePath: string,
  state: JwtRotationState,
): Promise<void> => {
  const normalized = normalizeState(state);
  if (!normalized) {
    throw new Error('Invalid jwt rotation state');
  }

  await mkdir(dirname(storePath), { recursive: true });
  await writeFile(storePath, `${JSON.stringify(normalized, null, 2)}\n`, 'utf-8');
};

export const isGracePeriodElapsed = (
  state: JwtRotationState,
  nowMs: number = Date.now(),
): boolean => {
  const rotatedAt = Date.parse(state.rotated_at);
  if (Number.isNaN(rotatedAt)) {
    return true;
  }
  return nowMs >= rotatedAt + state.grace_seconds * 1000;
};

export const createRotatedJwtState = (
  nextSignSecret: string,
  previousSignSecret: string,
  previousVerifySecrets: readonly string[],
  graceSeconds: number,
  nowMs: number = Date.now(),
): JwtRotationState => {
  const effectiveGrace = graceSeconds > 0 ? Math.floor(graceSeconds) : 86400;
  return {
    current_sign_secret: nextSignSecret,
    verify_secrets: normalizeVerifySecrets(nextSignSecret, [previousSignSecret, ...previousVerifySecrets]),
    rotated_at: new Date(nowMs).toISOString(),
    grace_seconds: effectiveGrace,
  };
};

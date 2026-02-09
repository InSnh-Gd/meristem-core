type EnvMap = Readonly<Record<string, string | undefined>>;

type PaginationInput = {
  limit?: number;
  offset?: number;
};

type PaginationPolicy = {
  defaultLimit: number;
  maxLimit: number;
  maxOffset?: number;
};

const QUERY_TIMEOUT_ENV = 'MERISTEM_DATABASE_QUERY_MAX_TIME_MS';
const DEFAULT_QUERY_MAX_TIME_MS = 3_000;
const MIN_QUERY_MAX_TIME_MS = 100;
const MAX_QUERY_MAX_TIME_MS = 60_000;
const DEFAULT_MAX_OFFSET = 50_000;

const toInteger = (value: number | undefined, fallback: number): number => {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return fallback;
  }
  return Math.trunc(value);
};

const clamp = (value: number, min: number, max: number): number => {
  if (value < min) {
    return min;
  }
  if (value > max) {
    return max;
  }
  return value;
};

export const resolveQueryMaxTimeMs = (
  env: EnvMap = process.env,
): number => {
  const raw = env[QUERY_TIMEOUT_ENV];
  const parsed = Number.parseInt(raw ?? '', 10);
  if (!Number.isFinite(parsed)) {
    return DEFAULT_QUERY_MAX_TIME_MS;
  }
  return clamp(parsed, MIN_QUERY_MAX_TIME_MS, MAX_QUERY_MAX_TIME_MS);
};

export const normalizePagination = (
  input: PaginationInput,
  policy: PaginationPolicy,
): { limit: number; offset: number } => {
  const maxLimit = Math.max(1, Math.trunc(policy.maxLimit));
  const defaultLimit = clamp(
    Math.max(1, Math.trunc(policy.defaultLimit)),
    1,
    maxLimit,
  );
  const maxOffset =
    typeof policy.maxOffset === 'number' && Number.isFinite(policy.maxOffset)
      ? Math.max(0, Math.trunc(policy.maxOffset))
      : DEFAULT_MAX_OFFSET;

  const limit = clamp(toInteger(input.limit, defaultLimit), 1, maxLimit);
  const offset = clamp(toInteger(input.offset, 0), 0, maxOffset);

  return { limit, offset };
};

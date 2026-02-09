import { DomainError } from '../errors/domain-error';

type EnvMap = Readonly<Record<string, string | undefined>>;

type PaginationInput = {
  limit?: number;
  offset?: number;
};

type CursorPaginationInput = {
  limit?: number;
  cursor?: string;
};

type PaginationPolicy = {
  defaultLimit: number;
  maxLimit: number;
  maxOffset?: number;
};

type CursorPaginationPolicy = {
  defaultLimit: number;
  maxLimit: number;
};

type CreatedAtCursorPayload = {
  created_at: string;
  tie_breaker: string;
};

type SequenceCursorPayload = {
  sequence: number;
};

export type CreatedAtCursor = {
  createdAt: Date;
  tieBreaker: string;
};

export type SequenceCursor = {
  sequence: number;
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

const parseCursor = (cursor: string): Record<string, unknown> => {
  try {
    const decoded = Buffer.from(cursor, 'base64url').toString('utf8');
    const parsed = JSON.parse(decoded);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new DomainError('INVALID_CURSOR');
    }
    return parsed as Record<string, unknown>;
  } catch (error) {
    if (error instanceof DomainError) {
      throw error;
    }
    throw new DomainError('INVALID_CURSOR', { cause: error });
  }
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

export const encodeCreatedAtCursor = (cursor: CreatedAtCursor): string =>
  Buffer.from(
    JSON.stringify({
      created_at: cursor.createdAt.toISOString(),
      tie_breaker: cursor.tieBreaker,
    } satisfies CreatedAtCursorPayload),
    'utf8',
  ).toString('base64url');

export const decodeCreatedAtCursor = (cursor: string): CreatedAtCursor => {
  const parsed = parseCursor(cursor);
  const createdAtRaw = parsed.created_at;
  const tieBreakerRaw = parsed.tie_breaker;

  if (typeof createdAtRaw !== 'string' || typeof tieBreakerRaw !== 'string') {
    throw new DomainError('INVALID_CURSOR');
  }

  const createdAtMs = Date.parse(createdAtRaw);
  if (!Number.isFinite(createdAtMs)) {
    throw new DomainError('INVALID_CURSOR');
  }

  if (tieBreakerRaw.length === 0) {
    throw new DomainError('INVALID_CURSOR');
  }

  return {
    createdAt: new Date(createdAtMs),
    tieBreaker: tieBreakerRaw,
  };
};

export const normalizeCursorPagination = (
  input: CursorPaginationInput,
  policy: CursorPaginationPolicy,
): { limit: number; cursor: string | null } => {
  const maxLimit = Math.max(1, Math.trunc(policy.maxLimit));
  const defaultLimit = clamp(
    Math.max(1, Math.trunc(policy.defaultLimit)),
    1,
    maxLimit,
  );
  const limit = clamp(toInteger(input.limit, defaultLimit), 1, maxLimit);

  if (typeof input.cursor !== 'string' || input.cursor.length === 0) {
    return { limit, cursor: null };
  }

  return {
    limit,
    cursor: input.cursor,
  };
};

export const encodeSequenceCursor = (cursor: SequenceCursor): string =>
  Buffer.from(
    JSON.stringify({ sequence: cursor.sequence } satisfies SequenceCursorPayload),
    'utf8',
  ).toString('base64url');

export const decodeSequenceCursor = (cursor: string): SequenceCursor => {
  const parsed = parseCursor(cursor);
  const sequence = parsed.sequence;
  if (
    typeof sequence !== 'number' ||
    !Number.isFinite(sequence) ||
    !Number.isInteger(sequence) ||
    sequence < 0
  ) {
    throw new DomainError('INVALID_CURSOR');
  }
  return { sequence };
};

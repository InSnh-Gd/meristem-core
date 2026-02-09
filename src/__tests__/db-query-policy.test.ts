import { expect, test } from 'bun:test';
import {
  decodeCreatedAtCursor,
  decodeSequenceCursor,
  encodeCreatedAtCursor,
  encodeSequenceCursor,
  normalizeCursorPagination,
  normalizePagination,
  resolveQueryMaxTimeMs,
} from '../db/query-policy';

test('normalizePagination clamps limit and offset within policy bounds', (): void => {
  const pagination = normalizePagination(
    { limit: 5000, offset: -42 },
    { defaultLimit: 100, maxLimit: 500, maxOffset: 10_000 },
  );

  expect(pagination).toEqual({
    limit: 500,
    offset: 0,
  });
});

test('normalizePagination falls back to defaults for invalid numeric inputs', (): void => {
  const pagination = normalizePagination(
    { limit: Number.NaN, offset: Number.POSITIVE_INFINITY },
    { defaultLimit: 120, maxLimit: 500 },
  );

  expect(pagination).toEqual({
    limit: 120,
    offset: 0,
  });
});

test('resolveQueryMaxTimeMs uses default when env is missing or invalid', (): void => {
  const missing = resolveQueryMaxTimeMs({});
  const invalid = resolveQueryMaxTimeMs({
    MERISTEM_DATABASE_QUERY_MAX_TIME_MS: 'not-a-number',
  });

  expect(missing).toBe(3000);
  expect(invalid).toBe(3000);
});

test('resolveQueryMaxTimeMs clamps env values to supported range', (): void => {
  const low = resolveQueryMaxTimeMs({
    MERISTEM_DATABASE_QUERY_MAX_TIME_MS: '10',
  });
  const high = resolveQueryMaxTimeMs({
    MERISTEM_DATABASE_QUERY_MAX_TIME_MS: '999999',
  });
  const normal = resolveQueryMaxTimeMs({
    MERISTEM_DATABASE_QUERY_MAX_TIME_MS: '5000',
  });

  expect(low).toBe(100);
  expect(high).toBe(60000);
  expect(normal).toBe(5000);
});

test('created-at cursor roundtrip preserves timestamp and tie breaker', (): void => {
  const encoded = encodeCreatedAtCursor({
    createdAt: new Date('2026-02-09T10:20:30.000Z'),
    tieBreaker: 'u-123',
  });

  const decoded = decodeCreatedAtCursor(encoded);
  expect(decoded.createdAt.toISOString()).toBe('2026-02-09T10:20:30.000Z');
  expect(decoded.tieBreaker).toBe('u-123');
});

test('sequence cursor roundtrip preserves sequence', (): void => {
  const encoded = encodeSequenceCursor({ sequence: 42 });
  const decoded = decodeSequenceCursor(encoded);
  expect(decoded.sequence).toBe(42);
});

test('normalizeCursorPagination clamps limit and accepts cursor token', (): void => {
  const pagination = normalizeCursorPagination(
    {
      limit: 999,
      cursor: 'cursor-token',
    },
    {
      defaultLimit: 100,
      maxLimit: 500,
    },
  );

  expect(pagination).toEqual({
    limit: 500,
    cursor: 'cursor-token',
  });
});

test('invalid cursor payload throws INVALID_CURSOR', (): void => {
  expect(() => decodeCreatedAtCursor('invalid-cursor')).toThrow('INVALID_CURSOR');
  expect(() => decodeSequenceCursor('invalid-cursor')).toThrow('INVALID_CURSOR');
});

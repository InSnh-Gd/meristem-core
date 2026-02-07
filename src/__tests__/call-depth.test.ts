import { expect, test } from 'bun:test';
import {
  CALL_DEPTH_HEADER,
  DEFAULT_CALL_DEPTH,
  MAX_CALL_DEPTH,
  validateCallDepth,
  validateCallDepthFromHeaders,
} from '../utils/call-depth';

test('validateCallDepth returns default depth when header is missing', (): void => {
  const result = validateCallDepth(null);
  expect(result).toEqual({
    ok: true,
    depth: DEFAULT_CALL_DEPTH,
  });
});

test('validateCallDepth accepts integer depth within range', (): void => {
  const result = validateCallDepth('12');
  expect(result).toEqual({
    ok: true,
    depth: 12,
  });
});

test('validateCallDepth rejects non-integer value', (): void => {
  const result = validateCallDepth('1.2');
  expect(result).toEqual({
    ok: false,
    reason: 'CALL_DEPTH_NOT_INTEGER',
    raw: '1.2',
  });
});

test('validateCallDepth rejects values above max', (): void => {
  const tooDeep = String(MAX_CALL_DEPTH + 1);
  const result = validateCallDepth(tooDeep);
  expect(result).toEqual({
    ok: false,
    reason: 'CALL_DEPTH_EXCEEDED',
    raw: tooDeep,
  });
});

test('validateCallDepthFromHeaders reads x-call-depth header', (): void => {
  const headers = new Headers();
  headers.set(CALL_DEPTH_HEADER, '3');
  const result = validateCallDepthFromHeaders(headers);
  expect(result).toEqual({
    ok: true,
    depth: 3,
  });
});

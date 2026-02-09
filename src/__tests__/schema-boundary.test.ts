import { expect, test } from 'bun:test';
import { Effect } from 'effect';
import {
  decodeHeartbeatBoundary,
  decodeJsonBoundary,
  decodePulseBoundary,
  runBoundarySync,
} from '../services/schema-boundary';

const runSync = <T>(program: Effect.Effect<T, unknown>): T =>
  Effect.runSync(program);

test('decodeHeartbeatBoundary fast path accepts valid payload', (): void => {
  const decoded = runSync(
    decodeHeartbeatBoundary(
      {
        node_id: 'node-a',
        ts: Date.now(),
        v: 1,
      },
      true,
    ),
  );

  expect(decoded.node_id).toBe('node-a');
});

test('decodeHeartbeatBoundary schema path rejects malformed payload', (): void => {
  const result = runBoundarySync(
    decodeHeartbeatBoundary(
      {
        node_id: 'node-a',
        ts: 'invalid-ts',
        v: 1,
      },
      false,
    ),
  );

  expect(result.ok).toBe(false);
});

test('decodePulseBoundary schema path parses valid payload', (): void => {
  const decoded = runSync(
    decodePulseBoundary(
      {
        node_id: 'node-pulse',
        ts: Date.now(),
        core: {
          cpu_load: 0.52,
          ram_usage: 0.74,
          net_io: {
            in: 10,
            out: 20,
          },
        },
      },
      false,
    ),
  );

  expect(decoded.node_id).toBe('node-pulse');
  expect(decoded.core.net_io?.in).toBe(10);
});

test('decodeJsonBoundary rejects invalid json bytes', (): void => {
  const invalid = new TextEncoder().encode('{invalid-json');
  const result = runBoundarySync(decodeJsonBoundary(invalid, 'test'));
  expect(result.ok).toBe(false);
});


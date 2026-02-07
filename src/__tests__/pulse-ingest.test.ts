import { expect, test } from 'bun:test';
import { createPulseMessageHandler, decodePulseMessage } from '../services/pulse-ingest';
import type { TraceContext } from '../utils/trace-context';

type LoggedEntry = {
  level: 'info' | 'warn' | 'error';
  message: string;
  meta?: Record<string, unknown>;
};

const encode = (value: unknown): Uint8Array => new TextEncoder().encode(JSON.stringify(value));

const traceContext: TraceContext = Object.freeze({
  traceId: 'trace-pulse-test',
  nodeId: 'core',
  source: 'test',
});

test('decodePulseMessage returns null for invalid payload', (): void => {
  const decoded = decodePulseMessage({ data: encode({ node_id: 'node-1' }) });
  expect(decoded).toBeNull();
});

test('decodePulseMessage returns normalized payload for valid pulse', (): void => {
  const decoded = decodePulseMessage({
    data: encode({
      node_id: 'node-1',
      ts: Date.now(),
      core: {
        cpu_load: 0.5,
        ram_usage: 0.7,
        net_io: {
          in: 100,
          out: 200,
        },
      },
      plugins: {
        gpu: { usage: 0.3 },
      },
    }),
  });

  expect(decoded).not.toBeNull();
  expect(decoded?.node_id).toBe('node-1');
  expect(decoded?.core.cpu_load).toBe(0.5);
});

test('createPulseMessageHandler writes snapshot log for valid pulse', async (): Promise<void> => {
  const loggedEntries: LoggedEntry[] = [];

  const handler = createPulseMessageHandler({
    createLogger: () => ({
      info: (message: string, meta?: Record<string, unknown>): void => {
        loggedEntries.push({ level: 'info', message, meta });
      },
      warn: (message: string, meta?: Record<string, unknown>): void => {
        loggedEntries.push({ level: 'warn', message, meta });
      },
      error: (message: string, meta?: Record<string, unknown>): void => {
        loggedEntries.push({ level: 'error', message, meta });
      },
    }),
  });

  const handled = await handler(traceContext, {
    data: encode({
      node_id: 'node-1',
      ts: Date.now(),
      core: {
        cpu_load: 0.5,
        ram_usage: 0.7,
        net_io: {
          in: 100,
          out: 200,
        },
      },
    }),
  });

  expect(handled).toBe(true);
  expect(loggedEntries).toHaveLength(1);
  expect(loggedEntries[0]?.level).toBe('info');
  expect(loggedEntries[0]?.message).toBe('[Pulse] Snapshot ingested');
  expect(loggedEntries[0]?.meta?.triad_type).toBe('snapshot');
});

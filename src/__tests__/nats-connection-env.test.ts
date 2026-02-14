import { afterEach, expect, mock, test } from 'bun:test';
import type { TraceContext } from '../utils/trace-context';

type ConnectOptionsLike = Readonly<{
  servers?: string | readonly string[];
  token?: string;
  timeout?: number;
}>;

const originalEnv = {
  MERISTEM_NATS_URL: process.env.MERISTEM_NATS_URL,
  MERISTEM_NATS_TOKEN: process.env.MERISTEM_NATS_TOKEN,
  NATS_URL: process.env.NATS_URL,
  NATS_TOKEN: process.env.NATS_TOKEN,
};

const restoreEnv = (): void => {
  for (const [key, value] of Object.entries(originalEnv)) {
    if (value === undefined) {
      delete process.env[key];
      continue;
    }
    process.env[key] = value;
  }
};

afterEach((): void => {
  restoreEnv();
  mock.restore();
});

test('connectNats prefers MERISTEM_NATS_URL and MERISTEM_NATS_TOKEN over legacy env vars', async () => {
  const connectCalls: ConnectOptionsLike[] = [];
  const loggerInfo = mock(() => {});
  const loggerError = mock(() => {});

  mock.module('nats', () => ({
    connect: mock(async (options: ConnectOptionsLike) => {
      connectCalls.push(options);
      return {
        close: mock(async () => {}),
      } as unknown;
    }),
  }));

  mock.module('../utils/logger', () => ({
    createLogger: () => ({
      info: loggerInfo,
      error: loggerError,
    }),
  }));

  process.env.MERISTEM_NATS_URL = 'nats://modern-host:4222';
  process.env.MERISTEM_NATS_TOKEN = 'modern-token';
  process.env.NATS_URL = 'nats://legacy-host:4333';
  process.env.NATS_TOKEN = 'legacy-token';

  const { closeNats, connectNats } = await import('../nats/connection');
  const traceContext: TraceContext = {
    traceId: 'trace-nats-env-test',
    nodeId: 'core-test-node',
    source: 'test',
  };

  await connectNats(traceContext);

  expect(connectCalls).toHaveLength(1);
  expect(connectCalls[0]?.servers).toBe('nats://modern-host:4222');
  expect(connectCalls[0]?.token).toBe('modern-token');

  await closeNats(traceContext);
});

import { describe, it, expect } from 'bun:test';
import { createNatsTransport, type LogEnvelope } from '../utils/nats-transport';

const createEnvelope = (overrides: Partial<LogEnvelope> = {}): LogEnvelope => {
  const base: LogEnvelope = {
    ts: 1700000000000,
    level: 'INFO',
    node_id: 'node-1',
    source: 'unit-test',
    trace_id: 'trace-1',
    content: 'test',
    meta: {},
  };

  const meta = overrides.meta ?? base.meta;
  return Object.freeze({ ...base, ...overrides, meta });
};

describe('NATS Transport', () => {
  it('publishes each entry and routes subjects correctly', async () => {
    const publishCalls: Array<readonly [string, Uint8Array]> = [];
    const connection = {
      publish: async (subject: string, payload: Uint8Array): Promise<void> => {
        publishCalls.push([subject, payload]);
      },
      jetstreamManager: async (): Promise<unknown> => ({}),
    };

    const transport = createNatsTransport({
      getConnection: async () => connection,
      minBatchSize: 50,
      maxBatchSize: 100,
      flushIntervalMs: 1000,
    });

    const logs = Array.from({ length: 60 }, (_value, index) =>
      createEnvelope({
        content: `log-${index}`,
        meta: index === 0 ? { taskId: 'task-1' } : {},
      }),
    );

    for (const log of logs) {
      transport.write(log);
    }

    await transport.flush(true);

    expect(publishCalls.length).toBe(60);
    expect(publishCalls[0]?.[0]).toBe('meristem.v1.logs.task.node-1.task-1');
    expect(publishCalls[1]?.[0]).toBe('meristem.v1.logs.sys.node-1');
  });

  it('flushes partial batches after timeout', async () => {
    const publishCalls: Array<readonly [string, Uint8Array]> = [];
    const connection = {
      publish: async (subject: string, payload: Uint8Array): Promise<void> => {
        publishCalls.push([subject, payload]);
      },
      jetstreamManager: async (): Promise<unknown> => ({}),
    };

    const transport = createNatsTransport({
      getConnection: async () => connection,
      minBatchSize: 50,
      maxBatchSize: 100,
      flushIntervalMs: 20,
    });

    transport.write(createEnvelope({ content: 'single-log' }));

    await new Promise((resolve) => setTimeout(resolve, 40));
    await transport.flush(true);

    expect(publishCalls.length).toBe(1);
  });

  it('drops oldest entries when buffer exceeds max bytes', async () => {
    const publishCalls: Array<readonly [string, Uint8Array]> = [];
    const connection = {
      publish: async (subject: string, payload: Uint8Array): Promise<void> => {
        publishCalls.push([subject, payload]);
      },
      jetstreamManager: async (): Promise<unknown> => ({}),
    };

    const encoder = new TextEncoder();
    const baseLog = createEnvelope({ content: 'log-000' });
    const entrySize = encoder.encode(JSON.stringify(baseLog)).byteLength;
    const bufferMaxBytes = entrySize * 3 + Math.floor(entrySize / 2);

    const transport = createNatsTransport({
      getConnection: async () => connection,
      minBatchSize: 10,
      maxBatchSize: 100,
      flushIntervalMs: 1000,
      bufferMaxBytes,
    });

    const logs = ['log-001', 'log-002', 'log-003', 'log-004'].map((content) =>
      createEnvelope({ content }),
    );

    for (const log of logs) {
      transport.write(log);
    }

    const stats = transport.stats();
    expect(stats.bufferedCount).toBe(3);
    expect(stats.droppedCount).toBe(1);

    await transport.flush(true);

    const decoder = new TextDecoder();
    const publishedContents = publishCalls.map(([_, payload]) => {
      const parsed = JSON.parse(decoder.decode(payload)) as LogEnvelope;
      return parsed.content;
    });

    expect(publishCalls.length).toBe(3);
    expect(publishedContents).toEqual(['log-002', 'log-003', 'log-004']);
  });

  it('falls back to core publish when JetStream is unavailable', async () => {
    const publishCalls: Array<readonly [string, Uint8Array]> = [];
    const connection = {
      publish: async (subject: string, payload: Uint8Array): Promise<void> => {
        publishCalls.push([subject, payload]);
      },
      jetstreamManager: async (): Promise<unknown> => {
        throw new Error('JetStream not enabled');
      },
    };

    const transport = createNatsTransport({
      getConnection: async () => connection,
      minBatchSize: 2,
      maxBatchSize: 100,
      flushIntervalMs: 1000,
    });

    transport.write(createEnvelope({ content: 'log-a' }));
    transport.write(createEnvelope({ content: 'log-b' }));

    await transport.flush(true);

    expect(publishCalls.length).toBe(2);
    expect(transport.stats().jetStreamAvailable).toBe(false);
  });
});

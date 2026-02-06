import { test, expect, mock, beforeEach } from 'bun:test';
import { setupJetstreamLogs, createLogConsumer } from '../services/jetstream-setup';
import { getNats, closeNats } from '../nats/connection';
import { getStreamReplicas } from '../config';

const STREAM_NAME = 'MERISTEM_LOGS';

beforeEach(async () => {
  await closeNats();
});

test('setupJetstreamLogs creates stream with correct configuration', async () => {
  const mockJsm = {
    streams: {
      add: mock(async (config: unknown) => {
        expect(config).toMatchObject({
          name: STREAM_NAME,
          subjects: ['meristem.v1.logs.sys.>', 'meristem.v1.logs.task.>'],
          max_age: 604800000000000,
          max_bytes: 10737418240,
          discard: 'old',
          retention: 'limits',
          num_replicas: 1,
          duplicate_window: 120000000000,
          max_msg_size: 1048576,
          max_msgs: -1,
        });
      }),
      info: mock(async () => ({
        config: {
          subjects: ['meristem.v1.logs.sys.>', 'meristem.v1.logs.task.>'],
          num_replicas: 1,
        },
      })),
    },
  };

  const mockNc = {
    jetstreamManager: mock(async () => mockJsm),
  };

  mock.module('../nats/connection', () => ({
    getNats: mock(async () => mockNc),
    closeNats: mock(async () => {}),
  }));

  mock.module('../config', () => ({
    getStreamReplicas: mock(() => 1),
  }));

  const result = await setupJetstreamLogs();
  expect(result).toBe(true);
  expect(mockJsm.streams.add).toHaveBeenCalled();
});

test('setupJetstreamLogs handles existing stream gracefully', async () => {
  const mockJsm = {
    streams: {
      add: mock(async () => {
        throw new Error('stream name already in use');
      }),
      info: mock(async () => ({
        config: {
          subjects: ['meristem.v1.logs.sys.>', 'meristem.v1.logs.task.>'],
          replicas: 1,
        },
      })),
    },
  };

  const mockNc = {
    jetstreamManager: mock(async () => mockJsm),
  };

  mock.module('../nats/connection', () => ({
    getNats: mock(async () => mockNc),
    closeNats: mock(async () => {}),
  }));

  mock.module('../config', () => ({
    getStreamReplicas: mock(() => 1),
  }));

  const result = await setupJetstreamLogs();
  expect(result).toBe(true);
  expect(mockJsm.streams.info).toHaveBeenCalledWith(STREAM_NAME);
});

test('setupJetstreamLogs returns false on JetStream unavailability', async () => {
  const mockNc = {
    jetstreamManager: mock(async () => {
      throw new Error('JetStream not enabled');
    }),
  };

  mock.module('../nats/connection', () => ({
    getNats: mock(async () => mockNc),
    closeNats: mock(async () => {}),
  }));

  mock.module('../config', () => ({
    getStreamReplicas: mock(() => 1),
  }));

  const result = await setupJetstreamLogs();
  expect(result).toBe(false);
});

test('setupJetstreamLogs uses configured replicas', async () => {
  const mockJsm = {
    streams: {
      add: mock(async (config: unknown) => {
        expect((config as { num_replicas: number }).num_replicas).toBe(3);
      }),
      info: mock(async () => ({
        config: {
          subjects: ['meristem.v1.logs.sys.>', 'meristem.v1.logs.task.>'],
          num_replicas: 3,
        },
      })),
    },
  };

  const mockNc = {
    jetstreamManager: mock(async () => mockJsm),
  };

  mock.module('../nats/connection', () => ({
    getNats: mock(async () => mockNc),
    closeNats: mock(async () => {}),
  }));

  mock.module('../config', () => ({
    getStreamReplicas: mock(() => 3),
  }));

  const result = await setupJetstreamLogs();
  expect(result).toBe(true);
});

test('createLogConsumer creates consumer with correct configuration', async () => {
  const mockJsm = {
    consumers: {
      add: mock(async (_streamName: string, config: unknown) => {
        expect(config).toMatchObject({
          name: 'test-consumer',
          durable_name: 'test-consumer',
          ack_policy: 'explicit',
          max_deliver: 3,
          replay_policy: 'instant',
        });
      }),
    },
  };

  const mockNc = {
    jetstreamManager: mock(async () => mockJsm),
  };

  mock.module('../nats/connection', () => ({
    getNats: mock(async () => mockNc),
    closeNats: mock(async () => {}),
  }));

  const result = await createLogConsumer('test-consumer');
  expect(result).not.toBeNull();
  expect(mockJsm.consumers.add).toHaveBeenCalledWith(STREAM_NAME, expect.any(Object));
});

test('createLogConsumer handles existing consumer gracefully', async () => {
  const mockJsm = {
    consumers: {
      add: mock(async () => {
        throw new Error('consumer already exists');
      }),
    },
  };

  const mockNc = {
    jetstreamManager: mock(async () => mockJsm),
  };

  mock.module('../nats/connection', () => ({
    getNats: mock(async () => mockNc),
    closeNat: mock(async () => {}),
  }));

  const result = await createLogConsumer('existing-consumer');
  expect(result).not.toBeNull();
});

test('createLogConsumer returns null on failure', async () => {
  const mockNc = {
    jetstreamManager: mock(async () => {
      throw new Error('Stream not found');
    }),
  };

  mock.module('../nats/connection', () => ({
    getNats: mock(async () => mockNc),
    closeNats: mock(async () => {}),
  }));

  const result = await createLogConsumer('test-consumer');
  expect(result).toBeNull();
});

test('createLogConsumer uses default consumer name', async () => {
  const mockJsm = {
    consumers: {
      add: mock(async (_streamName: string, config: unknown) => {
        expect((config as { name: string }).name).toBe('log-consumer');
      }),
    },
  };

  const mockNc = {
    jetstreamManager: mock(async () => mockJsm),
  };

  mock.module('../nats/connection', () => ({
    getNats: mock(async () => mockNc),
    closeNats: mock(async () => {}),
  }));

  const result = await createLogConsumer();
  expect(result).not.toBeNull();
});

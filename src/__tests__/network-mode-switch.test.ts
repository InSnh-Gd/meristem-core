import { expect, test } from 'bun:test';
import {
  createNetworkModeManager,
  type NetworkModeChangedEvent,
} from '../services/network-mode-manager';

const wait = async (ms: number): Promise<void> => {
  await new Promise<void>((resolve) => setTimeout(resolve, ms));
};

test('network mode manager keeps DIRECT when plugin is absent', async (): Promise<void> => {
  const published: NetworkModeChangedEvent[] = [];

  const manager = createNetworkModeManager({
    pollIntervalMs: 20,
    inspectProviders: () => [],
    publishEvent: async (_subject, payload) => {
      published.push(payload);
    },
    broadcastEvent: () => {
      return;
    },
  });

  await manager.start();
  await wait(30);
  manager.stop();

  expect(manager.getMode()).toBe('DIRECT');
  expect(published).toHaveLength(0);
});

test('network mode manager switches to M-NET when plugin is running and healthy', async (): Promise<void> => {
  const published: NetworkModeChangedEvent[] = [];
  const broadcasted: NetworkModeChangedEvent[] = [];

  const manager = createNetworkModeManager({
    pollIntervalMs: 20,
    inspectProviders: () => [
      {
        pluginId: 'com.test.overlay',
        exportName: 'network-mode-status',
        running: true,
        healthy: true,
      },
    ],
    publishEvent: async (_subject, payload) => {
      published.push(payload);
    },
    broadcastEvent: (_topic, payload) => {
      broadcasted.push(payload);
    },
  });

  await manager.start();
  await wait(30);
  manager.stop();

  expect(manager.getMode()).toBe('M-NET');
  expect(published).toHaveLength(1);
  expect(broadcasted).toHaveLength(1);
  expect(published[0]).toMatchObject({
    from: 'DIRECT',
    to: 'M-NET',
    reason: 'plugin_enabled',
    health: 'healthy',
  });
});

test('network mode manager falls back to DIRECT when plugin becomes unhealthy', async (): Promise<void> => {
  const published: NetworkModeChangedEvent[] = [];

  let snapshot = {
    running: true,
    healthy: true,
  };

  const manager = createNetworkModeManager({
    pollIntervalMs: 20,
    inspectProviders: () => [
      {
        pluginId: 'com.test.overlay',
        exportName: 'network-mode-status',
        running: snapshot.running,
        healthy: snapshot.healthy,
      },
    ],
    publishEvent: async (_subject, payload) => {
      published.push(payload);
    },
    broadcastEvent: () => {
      return;
    },
  });

  await manager.start();
  await wait(30);
  snapshot = {
    running: true,
    healthy: false,
  };
  await wait(35);
  manager.stop();

  expect(manager.getMode()).toBe('DIRECT');
  expect(published).toHaveLength(2);
  expect(published[0]).toMatchObject({
    from: 'DIRECT',
    to: 'M-NET',
    reason: 'plugin_enabled',
  });
  expect(published[1]).toMatchObject({
    from: 'M-NET',
    to: 'DIRECT',
    reason: 'plugin_failure',
    health: 'unhealthy',
  });
});

test('network mode manager follows plugin proposal to downgrade to DIRECT', async (): Promise<void> => {
  const published: NetworkModeChangedEvent[] = [];

  let proposal: { mode: 'DIRECT' | 'M-NET' } | null = null;

  const manager = createNetworkModeManager({
    pollIntervalMs: 20,
    inspectProviders: () => [
      {
        pluginId: 'com.test.overlay',
        exportName: 'network-mode-status',
        running: true,
        healthy: true,
      },
    ],
    inspectProposal: () => proposal,
    publishEvent: async (_subject, payload) => {
      published.push(payload);
    },
    broadcastEvent: () => {
      return;
    },
  });

  await manager.start();
  await wait(30);
  proposal = { mode: 'DIRECT' };
  await wait(35);
  manager.stop();

  expect(manager.getMode()).toBe('DIRECT');
  expect(published).toHaveLength(2);
  expect(published[1]).toMatchObject({
    from: 'M-NET',
    to: 'DIRECT',
    reason: 'plugin_proposal',
  });
});

test('network mode manager ignores M-NET proposal when plugin is unhealthy', async (): Promise<void> => {
  const published: NetworkModeChangedEvent[] = [];

  const manager = createNetworkModeManager({
    pollIntervalMs: 20,
    inspectProviders: () => [
      {
        pluginId: 'com.test.overlay',
        exportName: 'network-mode-status',
        running: true,
        healthy: false,
      },
    ],
    inspectProposal: () => ({ mode: 'M-NET' }),
    publishEvent: async (_subject, payload) => {
      published.push(payload);
    },
    broadcastEvent: () => {
      return;
    },
  });

  await manager.start();
  await wait(30);
  manager.stop();

  expect(manager.getMode()).toBe('DIRECT');
  expect(published).toHaveLength(0);
});

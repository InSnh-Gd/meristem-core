import { afterEach, describe, expect, test } from 'bun:test';
import { readFile } from 'fs/promises';
import type { Db } from 'mongodb';
import type {
  MServiceResponse,
  PluginManifest,
  PluginMessage,
} from '@insnh-gd/meristem-shared';
import {
  destroyPlugin,
  getPluginInstance,
  initPlugin,
  loadPlugin,
  parseManifest,
  reloadPlugin,
  startPlugin,
  stopPlugin,
} from '../services/plugin-lifecycle';
import { MServiceRouter, ServiceRegistry } from '../services/m-service-router';
import { PluginContextBridge, type MessageBridge } from '../services/plugin-bridge';
import { PluginIsolateManager } from '../services/plugin-isolate';
import { setPluginPermissions } from '../services/plugin-permission';

type StoredPluginDoc = Record<string, unknown>;

type MockPluginCollection = {
  updateOne: (
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
    options?: Record<string, unknown>,
  ) => Promise<{ acknowledged: boolean }>;
  findOneAndUpdate: (
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
    options?: Record<string, unknown>,
  ) => Promise<StoredPluginDoc | null>;
};

const FIXTURE_MANIFEST_URL = new URL('./fixtures/test-plugin/plugin.json', import.meta.url);
const FIXTURE_ENTRY_PATH = new URL('./fixtures/test-plugin/src/index.ts', import.meta.url).pathname;

const managedPluginIds = new Set<string>();

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

const makePluginId = (label: string): string =>
  `com.meristem.test.${label}.${crypto.randomUUID().replaceAll('-', '')}`;

const getConfigVersion = (doc: StoredPluginDoc): number => {
  const config = doc.config;
  if (!isRecord(config) || typeof config.v !== 'number') {
    return 1;
  }

  return config.v;
};

const setConfigVersion = (doc: StoredPluginDoc, version: number): void => {
  const currentConfig = isRecord(doc.config) ? doc.config : {};
  doc.config = {
    ...currentConfig,
    v: version,
  };
};

const createMockDb = (): {
  db: Db;
  getStoredVersion: (pluginId: string) => number | undefined;
} => {
  const docs = new Map<string, StoredPluginDoc>();

  const collection: MockPluginCollection = {
    updateOne: async (filter, update): Promise<{ acknowledged: boolean }> => {
      const pluginId = typeof filter.plugin_id === 'string' ? filter.plugin_id : '';
      const current = docs.get(pluginId) ?? {
        plugin_id: pluginId,
        config: { v: 1 },
      };

      const next = {
        ...current,
        ...(isRecord(update.$set) ? update.$set : {}),
      };

      setConfigVersion(next, getConfigVersion(next));
      docs.set(pluginId, next);

      return { acknowledged: true };
    },
    findOneAndUpdate: async (filter, update): Promise<StoredPluginDoc | null> => {
      const pluginId = typeof filter.plugin_id === 'string' ? filter.plugin_id : '';
      const current = docs.get(pluginId);
      if (!current) {
        return null;
      }

      const increment =
        isRecord(update.$inc) && typeof update.$inc['config.v'] === 'number'
          ? update.$inc['config.v']
          : 0;

      const next = {
        ...current,
        ...(isRecord(update.$set) ? update.$set : {}),
      };

      setConfigVersion(next, getConfigVersion(current) + increment);
      docs.set(pluginId, next);
      return next;
    },
  };

  const db = {
    collection: (_name: string): MockPluginCollection => collection,
  } as unknown as Db;

  return {
    db,
    getStoredVersion: (pluginId: string): number | undefined => {
      const doc = docs.get(pluginId);
      if (!doc) {
        return undefined;
      }

      return getConfigVersion(doc);
    },
  };
};

const readFixtureManifestJson = async (): Promise<string> => {
  return await readFile(FIXTURE_MANIFEST_URL, 'utf-8');
};

const createFixtureManifest = async (pluginId: string): Promise<PluginManifest> => {
  const manifestJson = await readFixtureManifestJson();
  const parseResult = parseManifest(manifestJson);
  if (!parseResult.success) {
    throw new Error(`Fixture manifest is invalid: ${parseResult.errors.join(', ')}`);
  }

  return {
    ...parseResult.manifest,
    id: pluginId,
    name: `Test Plugin ${pluginId}`,
    events: [],
  };
};

const waitFor = async (
  predicate: () => boolean,
  timeoutMs: number,
  pollIntervalMs = 20,
): Promise<void> => {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await Bun.sleep(pollIntervalMs);
  }

  throw new Error(`Condition not met within ${timeoutMs}ms`);
};

class MockCrossPluginBridge {
  public readonly sentMessages: PluginMessage[] = [];

  public generateMessageId(): string {
    return `msg-${this.sentMessages.length + 1}`;
  }

  public async sendMessageAndWait(
    _worker: Worker,
    message: PluginMessage,
  ): Promise<{ payload: MServiceResponse }> {
    this.sentMessages.push(message);

    const invokePayload = isRecord(message.payload) ? message.payload : {};
    const invokeParams = isRecord(invokePayload.params) ? invokePayload.params : {};
    const servicePayload = isRecord(invokeParams.payload) ? invokeParams.payload : {};
    const profileId =
      typeof servicePayload.profileId === 'string' ? servicePayload.profileId : 'unknown';

    return {
      payload: {
        success: true,
        data: {
          profileId,
          servedBy: message.pluginId,
        },
      },
    };
  }
}

afterEach(async (): Promise<void> => {
  for (const pluginId of [...managedPluginIds]) {
    await destroyPlugin(pluginId);
    managedPluginIds.delete(pluginId);
  }
});

describe('Plugin E2E (simplified)', () => {
  test('runs plugin full lifecycle from parse to cleanup', async () => {
    const pluginId = makePluginId('lifecycle');
    const { db } = createMockDb();

    const parseResult = parseManifest(await readFixtureManifestJson());
    expect(parseResult.success).toBe(true);
    if (!parseResult.success) {
      throw new Error('Expected fixture manifest to be valid');
    }

    const manifest = {
      ...parseResult.manifest,
      id: pluginId,
      name: `Lifecycle ${pluginId}`,
      events: [],
    } satisfies PluginManifest;

    const loadResult = await loadPlugin(db, manifest, FIXTURE_ENTRY_PATH);
    expect(loadResult).toEqual({ success: true });
    managedPluginIds.add(pluginId);

    expect(getPluginInstance(pluginId)?.state).toBe('LOADED');

    const initResult = await initPlugin(pluginId);
    expect(initResult.success).toBe(true);
    expect(getPluginInstance(pluginId)?.state).toBe('STARTING');

    const startResult = await startPlugin(pluginId);
    expect(startResult.success).toBe(true);
    expect(getPluginInstance(pluginId)?.state).toBe('RUNNING');

    const stopResult = await stopPlugin(pluginId, 500);
    expect(stopResult.success).toBe(true);
    expect(getPluginInstance(pluginId)?.state).toBe('STOPPED');

    const destroyResult = await destroyPlugin(pluginId);
    expect(destroyResult.success).toBe(true);
    expect(getPluginInstance(pluginId)).toBeUndefined();
    managedPluginIds.delete(pluginId);
  });

  test('routes M-Service call from plugin A to plugin B', async () => {
    const pluginAId = makePluginId('caller');
    const pluginBId = makePluginId('provider');

    const registry = new ServiceRegistry();
    registry.register('plugin.b.profile', pluginBId, ['get']);

    const bridge = new MockCrossPluginBridge();
    const router = new MServiceRouter({
      registry,
      messageBridge: bridge as unknown as MessageBridge,
      resolveWorker: (pluginId) => (pluginId === pluginBId ? ({} as Worker) : undefined),
    });

    const contextBridge = new PluginContextBridge({ mServiceRouter: router });
    const pluginAManifest = await createFixtureManifest(pluginAId);
    const pluginBManifest = await createFixtureManifest(pluginBId);

    contextBridge.createContext(pluginBId, pluginBManifest);
    const pluginAContext = contextBridge.createContext(pluginAId, pluginAManifest);

    const result = await pluginAContext.callService('plugin.b.profile', 'get', {
      profileId: 'p-1',
    });

    expect(result).toEqual({
      profileId: 'p-1',
      servedBy: pluginBId,
    });

    expect(bridge.sentMessages).toHaveLength(1);

    const firstPayload = bridge.sentMessages[0]?.payload;
    if (!isRecord(firstPayload)) {
      throw new Error('Expected routed payload to be an object');
    }

    const params = firstPayload.params;
    if (!isRecord(params)) {
      throw new Error('Expected routed params to be an object');
    }

    expect(params.caller).toBe(pluginAId);
    expect(params.service).toBe('plugin.b.profile');
    expect(params.method).toBe('get');
  });

  test('returns ACCESS_DENIED when caller lacks plugin:access', async () => {
    const callerId = makePluginId('denied');
    const targetId = makePluginId('target');

    setPluginPermissions(callerId, ['node:read']);

    const registry = new ServiceRegistry();
    registry.register('plugin.b.secure', targetId, ['read']);

    const router = new MServiceRouter({
      registry,
      resolveWorker: () => ({} as Worker),
    });

    const response = await router.route({
      trace_id: 'trace-access-denied',
      caller: callerId,
      service: 'plugin.b.secure',
      method: 'read',
      payload: {},
      timeout: 200,
    });

    expect(response.success).toBe(false);
    if (response.success) {
      throw new Error('Expected ACCESS_DENIED response');
    }

    expect(response.error?.code).toBe('ACCESS_DENIED');
  });

  test('reloads plugin and increments config version', async () => {
    const pluginId = makePluginId('reload');
    const { db, getStoredVersion } = createMockDb();
    const manifest = await createFixtureManifest(pluginId);

    const loadResult = await loadPlugin(db, manifest, FIXTURE_ENTRY_PATH);
    expect(loadResult.success).toBe(true);
    managedPluginIds.add(pluginId);

    const startResult = await startPlugin(pluginId);
    expect(startResult.success).toBe(true);

    expect(getPluginInstance(pluginId)?.configVersion).toBe(1);
    expect(getStoredVersion(pluginId)).toBe(1);

    const reloadResult = await reloadPlugin(pluginId);
    expect(reloadResult.success).toBe(true);

    expect(getPluginInstance(pluginId)?.state).toBe('RUNNING');
    expect(getPluginInstance(pluginId)?.configVersion).toBe(2);
    expect(getStoredVersion(pluginId)).toBe(2);
  });

  test('recovers from simulated crash by auto-restarting isolate', async () => {
    const pluginId = makePluginId('crash');
    const isolateManager = new PluginIsolateManager({
      monitorIntervalMs: 20,
      maxRestarts: 2,
    });

    const manifest = await createFixtureManifest(pluginId);
    const isolate = await isolateManager.createIsolate(pluginId, manifest, FIXTURE_ENTRY_PATH);

    expect(isolateManager.isIsolateRunning(pluginId)).toBe(true);
    const beforeRestarts = isolateManager.getIsolateStats(pluginId).restarts;

    /**
     * 逻辑块：用“崩溃错误事件 + 进程终止”模拟 Worker 非预期退出。
     * - 目的：触发 IsolateManager 的异常恢复链路。
     * - 原因：测试环境里直接 dispatch error 比依赖真实运行时崩溃更稳定。
     * - 降级：即使 terminate 时序先后不同，error 事件仍会驱动自动重启判定。
     */
    isolate.worker.dispatchEvent(new ErrorEvent('error', { message: 'simulated crash' }));
    isolate.worker.terminate();

    await waitFor(() => isolateManager.getIsolateStats(pluginId).restarts > beforeRestarts, 1500);

    expect(isolateManager.isIsolateRunning(pluginId)).toBe(true);
    expect(isolateManager.getIsolateStats(pluginId).restarts).toBeGreaterThan(beforeRestarts);

    await isolateManager.destroyIsolate(pluginId);
    expect(isolateManager.isIsolateRunning(pluginId)).toBe(false);
  });
});

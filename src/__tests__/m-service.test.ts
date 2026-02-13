import { describe, expect, test } from 'bun:test';
import type { MServiceResponse, PluginMessage } from '@insnh-gd/meristem-shared';
import type { MessageBridge } from '../services/plugin-bridge';
import { setPluginPermissions } from '../services/plugin-permission';
import { MServiceRouter, ServiceRegistry } from '../services/m-service-router';

type BridgeHandler = (message: PluginMessage) => Promise<MServiceResponse>;

class MockMessageBridge {
  public readonly sentMessages: PluginMessage[] = [];

  public constructor(private readonly handler: BridgeHandler) {}

  public generateMessageId(): string {
    return 'msg-1';
  }

  public async sendMessageAndWait(
    _worker: Worker,
    message: PluginMessage,
    _timeoutMs?: number
  ): Promise<{ payload: MServiceResponse }> {
    this.sentMessages.push(message);
    return {
      payload: await this.handler(message),
    };
  }
}

const createWorker = (): Worker => ({}) as unknown as Worker;

describe('M-Service Router', () => {
  test('registers, looks up, and unregisters service', () => {
    const registry = new ServiceRegistry();

    registry.register('metrics.query', 'plugin.metrics', ['list', 'get']);

    expect(registry.lookup('metrics.query')).toEqual({
      service: 'metrics.query',
      pluginId: 'plugin.metrics',
      methods: ['list', 'get'],
    });

    registry.unregister('metrics.query');
    expect(registry.lookup('metrics.query')).toBeUndefined();
  });

  test('routes request to correct plugin and returns response', async () => {
    const registry = new ServiceRegistry();
    registry.register('plugin.b.health', 'plugin.b', ['check']);
    setPluginPermissions('plugin.a', ['plugin:access']);

    const bridge = new MockMessageBridge(async () => ({
      success: true,
      data: { status: 'ok' },
    }));

    const router = new MServiceRouter({
      registry,
      messageBridge: bridge as unknown as MessageBridge,
      resolveWorker: pluginId => (pluginId === 'plugin.b' ? createWorker() : undefined),
      defaultTimeoutMs: 5000,
    });

    const response = await router.route({
      trace_id: 'trace-1',
      caller: 'plugin.a',
      service: 'plugin.b.health',
      method: 'check',
      payload: { verbose: true },
      timeout: 5000,
    });

    expect(response).toEqual({
      success: true,
      data: { status: 'ok' },
    });

    expect(bridge.sentMessages).toHaveLength(1);
    const routed = bridge.sentMessages[0];
    expect(routed?.pluginId).toBe('plugin.b');
    expect(routed?.payload).toEqual({
      method: 'plugin.b.health.check',
      params: {
        trace_id: 'trace-1',
        caller: 'plugin.a',
        service: 'plugin.b.health',
        method: 'check',
        payload: { verbose: true },
      },
      timeout: 5000,
    });
  });

  test('returns TIMEOUT when request exceeds timeout', async () => {
    const registry = new ServiceRegistry();
    registry.register('plugin.b.slow', 'plugin.b', ['run']);
    setPluginPermissions('plugin.timeout.caller', ['plugin:access']);

    const bridge = new MockMessageBridge(
      () =>
        new Promise(resolve => {
          setTimeout(() => {
            resolve({ success: true, data: { done: true } });
          }, 30);
        })
    );

    const router = new MServiceRouter({
      registry,
      messageBridge: bridge as unknown as MessageBridge,
      resolveWorker: () => createWorker(),
      defaultTimeoutMs: 5000,
    });

    const response = await router.route({
      trace_id: 'trace-timeout',
      caller: 'plugin.timeout.caller',
      service: 'plugin.b.slow',
      method: 'run',
      payload: {},
      timeout: 5,
    });

    expect(response.success).toBe(false);
    if (!response.success) {
      expect(response.error?.code).toBe('TIMEOUT');
    }
  });

  test('returns SERVICE_UNAVAILABLE when service is not found', async () => {
    setPluginPermissions('plugin.lookup.caller', ['plugin:access']);

    const router = new MServiceRouter({
      registry: new ServiceRegistry(),
      resolveWorker: () => createWorker(),
      defaultTimeoutMs: 5000,
    });

    const response = await router.route({
      trace_id: 'trace-missing-service',
      caller: 'plugin.lookup.caller',
      service: 'plugin.missing.service',
      method: 'call',
      payload: {},
      timeout: 5000,
    });

    expect(response).toMatchObject({
      success: false,
      error: { code: 'SERVICE_UNAVAILABLE' },
    });
  });

  test('returns METHOD_NOT_FOUND when target plugin does not implement method', async () => {
    const registry = new ServiceRegistry();
    registry.register('plugin.b.math', 'plugin.b', []);
    setPluginPermissions('plugin.method.caller', ['plugin:access']);

    const bridge = new MockMessageBridge(async () => ({
      success: false,
      error: {
        code: 'METHOD_NOT_FOUND',
        message: 'method subtract is not implemented',
      },
    }));

    const router = new MServiceRouter({
      registry,
      messageBridge: bridge as unknown as MessageBridge,
      resolveWorker: () => createWorker(),
      defaultTimeoutMs: 5000,
    });

    const response = await router.route({
      trace_id: 'trace-missing-method',
      caller: 'plugin.method.caller',
      service: 'plugin.b.math',
      method: 'subtract',
      payload: { a: 4, b: 2 },
      timeout: 5000,
    });

    expect(response).toEqual({
      success: false,
      error: {
        code: 'METHOD_NOT_FOUND',
        message: 'method subtract is not implemented',
      },
    });
  });

  test('returns ACCESS_DENIED when caller has no plugin:access permission', async () => {
    const registry = new ServiceRegistry();
    registry.register('plugin.b.secure', 'plugin.b', ['read']);

    const router = new MServiceRouter({
      registry,
      resolveWorker: () => createWorker(),
      defaultTimeoutMs: 5000,
    });

    const response = await router.route({
      trace_id: 'trace-no-perm',
      caller: 'plugin.no.permission',
      service: 'plugin.b.secure',
      method: 'read',
      payload: {},
      timeout: 5000,
    });

    expect(response).toMatchObject({
      success: false,
      error: { code: 'ACCESS_DENIED' },
    });
  });

  test('supports cross-plugin call flow from plugin A to plugin B', async () => {
    const registry = new ServiceRegistry();
    registry.register('plugin.b.profile', 'plugin.b', ['get']);
    setPluginPermissions('plugin.a', ['plugin:access']);

    const bridge = new MockMessageBridge(async message => {
      expect(message.pluginId).toBe('plugin.b');
      return {
        success: true,
        data: {
          from: 'plugin.b',
          caller: 'plugin.a',
          profileId: 'p-1',
        },
      };
    });

    const router = new MServiceRouter({
      registry,
      messageBridge: bridge as unknown as MessageBridge,
      resolveWorker: pluginId => (pluginId === 'plugin.b' ? createWorker() : undefined),
      defaultTimeoutMs: 5000,
    });

    const response = await router.route({
      trace_id: 'trace-cross-plugin',
      caller: 'plugin.a',
      service: 'plugin.b.profile',
      method: 'get',
      payload: { profileId: 'p-1' },
      timeout: 5000,
    });

    expect(response).toEqual({
      success: true,
      data: {
        from: 'plugin.b',
        caller: 'plugin.a',
        profileId: 'p-1',
      },
    });
  });
});

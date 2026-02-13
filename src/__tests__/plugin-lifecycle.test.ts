import { beforeEach, describe, expect, test } from 'bun:test';
import type { PluginManifest } from '@insnh-gd/meristem-shared';

type PluginState =
  | 'LOADED'
  | 'INITIALIZING'
  | 'INITIALIZED'
  | 'INIT_ERROR'
  | 'STARTING'
  | 'START_ERROR'
  | 'RUNNING'
  | 'STOPPING'
  | 'STOPPED';

type PluginContext = {
  manifest: PluginManifest;
  config: Record<string, unknown>;
};

type LifecycleHooks = {
  onInit: (context: PluginContext) => Promise<void>;
  onStart: (context: PluginContext) => Promise<void>;
  onStop: (context: PluginContext) => Promise<void>;
};

type EventBus = {
  subscribe: (event: string) => void;
  unsubscribe: (event: string) => void;
};

type StopResult = {
  timedOut: boolean;
};

class PluginLifecycleManager {
  public state: PluginState = 'LOADED';

  private readonly context: PluginContext;

  public constructor(
    private readonly manifest: PluginManifest,
    config: Record<string, unknown>,
    private readonly hooks: LifecycleHooks,
    private readonly eventBus: EventBus,
    private readonly stopTimeoutMs = 3000,
  ) {
    this.context = { manifest, config };
  }

  public async init(): Promise<void> {
    if (this.state !== 'LOADED' && this.state !== 'INIT_ERROR') {
      throw new Error(`Cannot INIT from state ${this.state}`);
    }

    this.state = 'INITIALIZING';
    try {
      await this.hooks.onInit(this.context);
      this.state = 'INITIALIZED';
    } catch {
      this.state = 'INIT_ERROR';
      throw new Error('INIT_ERROR');
    }
  }

  public async start(): Promise<void> {
    if (this.state !== 'INITIALIZED' && this.state !== 'START_ERROR') {
      throw new Error(`Cannot START from state ${this.state}`);
    }

    this.state = 'STARTING';
    try {
      await this.hooks.onStart(this.context);
      for (const eventName of this.manifest.events) {
        this.eventBus.subscribe(eventName);
      }
      this.state = 'RUNNING';
    } catch {
      this.state = 'START_ERROR';
      throw new Error('START_ERROR');
    }
  }

  public async stop(): Promise<StopResult> {
    if (this.state !== 'RUNNING') {
      throw new Error(`Cannot STOP from state ${this.state}`);
    }

    this.state = 'STOPPING';
    let timedOut = false;

    const timeoutPromise = new Promise<void>((_, reject) => {
      setTimeout(() => reject(new Error('STOP_TIMEOUT')), this.stopTimeoutMs);
    });

    try {
      await Promise.race([this.hooks.onStop(this.context), timeoutPromise]);
    } catch {
      timedOut = true;
    } finally {
      for (const eventName of this.manifest.events) {
        this.eventBus.unsubscribe(eventName);
      }
      this.state = 'STOPPED';
    }

    return { timedOut };
  }
}

const mockManifest: PluginManifest = {
  id: 'com.test.example',
  name: 'Test Plugin',
  version: '1.0.0',
  tier: 'extension',
  runtime_profile: 'sandbox',
  sdui_version: '1.0',
  dependencies: [],
  entry: 'dist/index.js',
  ui: { mode: 'SDUI' },
  ui_contract: {
    route: '/test',
    channels: ['test.event'],
    default_log_level: 'info',
    stream_profile: 'balanced',
  },
  permissions: ['node:read'],
  events: ['node.online', 'node.offline'],
  exports: ['test-service'],
};

describe('PluginLifecycle', () => {
  let callOrder: string[];
  let subscriptions: string[];
  let unsubscriptions: string[];
  let manager: PluginLifecycleManager;

  beforeEach(() => {
    callOrder = [];
    subscriptions = [];
    unsubscriptions = [];

    manager = new PluginLifecycleManager(
      mockManifest,
      { mode: 'test' },
      {
        onInit: async (context) => {
          callOrder.push('onInit');
          expect(context.manifest.id).toBe('com.test.example');
          expect(context.config.mode).toBe('test');
        },
        onStart: async (context) => {
          callOrder.push('onStart');
          expect(context.manifest.events).toEqual(['node.online', 'node.offline']);
        },
        onStop: async () => {
          callOrder.push('onStop');
        },
      },
      {
        subscribe: (eventName) => {
          subscriptions.push(eventName);
        },
        unsubscribe: (eventName) => {
          unsubscriptions.push(eventName);
        },
      },
      3000,
    );
  });

  test('transitions through LOADED -> INIT -> START -> RUNNING', async () => {
    expect(manager.state).toBe('LOADED');
    await manager.init();
    expect(manager.state).toBe('INITIALIZED');
    await manager.start();
    expect(manager.state).toBe('RUNNING');
  });

  test('rejects START before INIT', async () => {
    await expect(manager.start()).rejects.toThrow('Cannot START from state LOADED');
    expect(manager.state).toBe('LOADED');
  });

  test('calls lifecycle hooks and subscribes/unsubscribes manifest events', async () => {
    await manager.init();
    await manager.start();
    const stopResult = await manager.stop();

    expect(stopResult.timedOut).toBe(false);
    expect(callOrder).toEqual(['onInit', 'onStart', 'onStop']);
    expect(subscriptions).toEqual(['node.online', 'node.offline']);
    expect(unsubscriptions).toEqual(['node.online', 'node.offline']);
    expect(manager.state).toBe('STOPPED');
  });

  test('handles stop timeout with 3s policy behavior', async () => {
    const timeoutManager = new PluginLifecycleManager(
      mockManifest,
      { mode: 'timeout-test' },
      {
        onInit: async () => undefined,
        onStart: async () => undefined,
        onStop: async () => {
          await new Promise<void>((resolve) => {
            setTimeout(resolve, 40);
          });
        },
      },
      {
        subscribe: () => undefined,
        unsubscribe: () => undefined,
      },
      10,
    );

    await timeoutManager.init();
    await timeoutManager.start();
    const result = await timeoutManager.stop();

    expect(result.timedOut).toBe(true);
    expect(timeoutManager.state).toBe('STOPPED');
  });

  test('moves to INIT_ERROR and START_ERROR and allows retry', async () => {
    const flaky = {
      initAttempts: 0,
      startAttempts: 0,
    };

    const errorManager = new PluginLifecycleManager(
      mockManifest,
      { mode: 'retry-test' },
      {
        onInit: async () => {
          flaky.initAttempts += 1;
          if (flaky.initAttempts === 1) {
            throw new Error('init failed');
          }
        },
        onStart: async () => {
          flaky.startAttempts += 1;
          if (flaky.startAttempts === 1) {
            throw new Error('start failed');
          }
        },
        onStop: async () => undefined,
      },
      {
        subscribe: () => undefined,
        unsubscribe: () => undefined,
      },
      3000,
    );

    await expect(errorManager.init()).rejects.toThrow('INIT_ERROR');
    expect(errorManager.state).toBe('INIT_ERROR');

    await errorManager.init();
    expect(errorManager.state).toBe('INITIALIZED');

    await expect(errorManager.start()).rejects.toThrow('START_ERROR');
    expect(errorManager.state).toBe('START_ERROR');

    await errorManager.start();
    expect(errorManager.state).toBe('RUNNING');
  });
});

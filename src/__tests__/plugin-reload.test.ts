import { beforeEach, describe, expect, test } from 'bun:test';

type PluginState = 'LOADED' | 'RUNNING' | 'STOPPED';

type ReloadResult = { success: true } | { success: false; error: string };

type WorkerBehavior = {
  onInit?: () => Promise<void>;
  onStart?: () => Promise<void>;
};

type MockWorker = {
  id: string;
  initCalls: number;
  startCalls: number;
  terminateCalls: number;
  initialized: boolean;
  running: boolean;
  terminated: boolean;
  init: () => Promise<void>;
  start: () => Promise<void>;
  terminate: () => Promise<void>;
  handleTraffic: (payload: string) => string;
};

type ReloadRuntime = {
  state: PluginState;
  configVersion: number;
  activeWorker: MockWorker;
  pendingWorker?: MockWorker;
  createdWorkers: MockWorker[];
  createWorker: () => MockWorker;
  routeTraffic: (payload: string) => string;
};

type Deferred = {
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: Error) => void;
};

const createDeferred = (): Deferred => {
  let resolve: (() => void) | undefined;
  let reject: ((error: Error) => void) | undefined;

  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = (error: Error) => rej(error);
  });

  if (!resolve || !reject) {
    throw new Error('Failed to create deferred promise');
  }

  return {
    promise,
    resolve,
    reject,
  };
};

const createMockWorker = (id: string, behavior: WorkerBehavior = {}): MockWorker => {
  const worker: MockWorker = {
    id,
    initCalls: 0,
    startCalls: 0,
    terminateCalls: 0,
    initialized: false,
    running: false,
    terminated: false,
    async init() {
      worker.initCalls += 1;
      await behavior.onInit?.();
      worker.initialized = true;
    },
    async start() {
      if (!worker.initialized) {
        throw new Error('worker not initialized');
      }

      worker.startCalls += 1;
      await behavior.onStart?.();
      worker.running = true;
    },
    async terminate() {
      worker.terminateCalls += 1;
      worker.running = false;
      worker.terminated = true;
    },
    handleTraffic(payload: string) {
      if (!worker.running || worker.terminated) {
        throw new Error(`worker ${worker.id} is not available`);
      }

      return `${worker.id}:${payload}`;
    },
  };

  return worker;
};

const createRuntime = (input?: {
  state?: PluginState;
  configVersion?: number;
  nextWorkerBehavior?: WorkerBehavior;
}): {
  runtime: ReloadRuntime;
  oldWorker: MockWorker;
  getNewWorker: () => MockWorker | undefined;
} => {
  const state = input?.state ?? 'RUNNING';
  const oldWorker = createMockWorker('worker-old');
  oldWorker.initialized = state === 'RUNNING';
  oldWorker.running = state === 'RUNNING';

  const createdWorkers: MockWorker[] = [oldWorker];
  let workerCounter = 1;
  const nextWorkerBehavior = input?.nextWorkerBehavior;

  const runtime: ReloadRuntime = {
    state,
    configVersion: input?.configVersion ?? 1,
    activeWorker: oldWorker,
    createdWorkers,
    createWorker: () => {
      workerCounter += 1;
      const worker = createMockWorker(`worker-new-${workerCounter}`, nextWorkerBehavior);
      createdWorkers.push(worker);
      return worker;
    },
    routeTraffic: (payload: string) => runtime.activeWorker.handleTraffic(payload),
  };

  return {
    runtime,
    oldWorker,
    getNewWorker: () => createdWorkers.at(1),
  };
};

const requireWorker = (worker: MockWorker | undefined, message: string): MockWorker => {
  if (!worker) {
    throw new Error(message);
  }

  return worker;
};

const reloadPlugin = async (runtime: ReloadRuntime): Promise<ReloadResult> => {
  if (runtime.state !== 'RUNNING') {
    return {
      success: false,
      error: `Cannot reload plugin in state ${runtime.state}`,
    };
  }

  const oldWorker = runtime.activeWorker;
  const nextWorker = runtime.createWorker();
  runtime.pendingWorker = nextWorker;

  try {
    await nextWorker.init();
    await nextWorker.start();

    runtime.activeWorker = nextWorker;
    runtime.pendingWorker = undefined;
    runtime.configVersion += 1;

    await oldWorker.terminate();
    return { success: true };
  } catch (error) {
    runtime.activeWorker = oldWorker;
    runtime.pendingWorker = undefined;
    await nextWorker.terminate();

    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
};

describe('Plugin Reload', () => {
  let runtime: ReloadRuntime;
  let oldWorker: MockWorker;
  let getNewWorker: () => MockWorker | undefined;

  beforeEach(() => {
    const initial = createRuntime();
    runtime = initial.runtime;
    oldWorker = initial.oldWorker;
    getNewWorker = initial.getNewWorker;
  });

  test('should reload plugin successfully', async () => {
    const result = await reloadPlugin(runtime);
    const newWorker = requireWorker(getNewWorker(), 'expected created worker after reload');

    expect(result).toEqual({ success: true });
    expect(runtime.activeWorker).toBe(newWorker);
    expect(newWorker.initCalls).toBe(1);
    expect(newWorker.startCalls).toBe(1);
    expect(oldWorker.terminateCalls).toBe(1);
    expect(oldWorker.terminated).toBe(true);
    expect(runtime.configVersion).toBe(2);
  });

  test('should rollback when new worker init fails', async () => {
    const failed = createRuntime({
      nextWorkerBehavior: {
        onInit: async () => {
          throw new Error('INIT_FAILED');
        },
      },
    });

    const result = await reloadPlugin(failed.runtime);
    const newWorker = failed.getNewWorker();

    expect(result.success).toBe(false);
    if (result.success) {
      throw new Error('expected init failure');
    }

    expect(result.error).toContain('INIT_FAILED');
    expect(failed.runtime.activeWorker).toBe(failed.oldWorker);
    expect(failed.oldWorker.running).toBe(true);
    expect(failed.oldWorker.terminateCalls).toBe(0);
    expect(newWorker?.startCalls).toBe(0);
    expect(newWorker?.terminateCalls).toBe(1);
    expect(failed.runtime.configVersion).toBe(1);
  });

  test('should rollback when new worker start fails', async () => {
    const failed = createRuntime({
      nextWorkerBehavior: {
        onStart: async () => {
          throw new Error('START_FAILED');
        },
      },
    });

    const result = await reloadPlugin(failed.runtime);
    const newWorker = failed.getNewWorker();

    expect(result.success).toBe(false);
    if (result.success) {
      throw new Error('expected start failure');
    }

    expect(result.error).toContain('START_FAILED');
    expect(newWorker?.initCalls).toBe(1);
    expect(newWorker?.startCalls).toBe(1);
    expect(failed.runtime.activeWorker).toBe(failed.oldWorker);
    expect(failed.oldWorker.running).toBe(true);
    expect(failed.oldWorker.terminateCalls).toBe(0);
    expect(newWorker?.terminateCalls).toBe(1);
    expect(failed.runtime.configVersion).toBe(1);
  });

  test('should reject reload when plugin is not in RUNNING state', async () => {
    const nonRunning = createRuntime({ state: 'LOADED' });

    const result = await reloadPlugin(nonRunning.runtime);

    expect(result.success).toBe(false);
    if (result.success) {
      throw new Error('expected invalid state failure');
    }

    expect(result.error).toContain('Cannot reload plugin in state LOADED');
    expect(nonRunning.runtime.createdWorkers).toHaveLength(1);
    expect(nonRunning.oldWorker.terminateCalls).toBe(0);
  });

  test('should keep traffic on old worker during transition and switch after success', async () => {
    const startGate = createDeferred();
    const staged = createRuntime({
      nextWorkerBehavior: {
        onStart: async () => startGate.promise,
      },
    });

    const reloadPromise = reloadPlugin(staged.runtime);
    const newWorker = requireWorker(
      staged.getNewWorker(),
      'expected new worker during in-flight reload',
    );

    expect(staged.runtime.pendingWorker).toBe(newWorker);
    expect(staged.oldWorker.terminated).toBe(false);
    expect(newWorker.terminated).toBe(false);
    expect(staged.runtime.routeTraffic('request-A')).toBe('worker-old:request-A');

    startGate.resolve();
    const result = await reloadPromise;

    expect(result).toEqual({ success: true });
    expect(staged.runtime.pendingWorker).toBeUndefined();
    expect(staged.runtime.routeTraffic('request-B')).toBe(`${newWorker.id}:request-B`);
    expect(staged.oldWorker.terminated).toBe(true);
  });
});

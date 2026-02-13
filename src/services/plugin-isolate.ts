import type { PluginManifest } from '@insnh-gd/meristem-shared';
import type { PluginMessage, PluginHealthReport } from '@insnh-gd/meristem-shared';

const DEFAULT_MAX_RESTARTS = 3;
const DEFAULT_MEMORY_THRESHOLD = 512 * 1024 * 1024;
const DEFAULT_MONITOR_INTERVAL_MS = 15_000;
const DEFAULT_RELOAD_GRACE_PERIOD_MS = 1_000;

type IsolateRuntime = {
  manifest: PluginManifest;
  entryPath: string;
  port: MessagePort;
  startedAt: number;
  memory?: number;
};

type WorkerHandlers = {
  message: (event: MessageEvent<unknown>) => void;
  error: (event: ErrorEvent) => void;
  messageerror: (event: MessageEvent<unknown>) => void;
};

type PortHandlers = {
  message: (event: MessageEvent<unknown>) => void;
  messageerror: (event: MessageEvent<unknown>) => void;
};

type ReloadWorkerState = {
  oldWorker: Worker;
  newWorker: Worker;
  active: 'old' | 'new';
};

export type WorkerPoolOptions = {
  smol?: boolean;
};

export type CircuitBreakerOptions = {
  maxRestarts?: number;
  memoryThreshold?: number;
};

export type PluginIsolateManagerOptions = {
  maxRestarts?: number;
  memoryThreshold?: number;
  monitorIntervalMs?: number;
  workerPool?: WorkerPool;
  circuitBreaker?: CircuitBreaker;
};

export type IsolateStats = {
  restarts: number;
  uptime: number;
  memory?: number;
};

const INIT_MESSAGE_TYPE = 'INIT' as PluginMessage['type'];
const HEALTH_MESSAGE_TYPE = 'HEALTH' as PluginMessage['type'];

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

const isPluginMessage = (value: unknown): value is PluginMessage => {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.id === 'string' &&
    typeof value.pluginId === 'string' &&
    typeof value.timestamp === 'number' &&
    typeof value.type === 'string'
  );
};

const isMemoryUsage = (value: unknown): value is NodeJS.MemoryUsage => {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.rss === 'number' &&
    typeof value.heapTotal === 'number' &&
    typeof value.heapUsed === 'number' &&
    typeof value.external === 'number' &&
    typeof value.arrayBuffers === 'number'
  );
};

const isPluginHealthReport = (value: unknown): value is PluginHealthReport => {
  if (!isRecord(value)) {
    return false;
  }

  return (
    isMemoryUsage(value.memoryUsage) &&
    typeof value.uptime === 'number' &&
    (value.status === 'healthy' ||
      value.status === 'degraded' ||
      value.status === 'unhealthy')
  );
};

const createInitMessage = (pluginId: string, manifest: PluginManifest): PluginMessage => ({
  id: crypto.randomUUID(),
  type: INIT_MESSAGE_TYPE,
  pluginId,
  timestamp: Date.now(),
  payload: { manifest },
});

export class WorkerPool {
  private readonly workers = new Map<string, Worker>();
  private readonly reloadWorkers = new Map<string, ReloadWorkerState>();
  private readonly entryPaths = new Map<string, string>();
  private readonly smol: boolean;

  constructor(options: WorkerPoolOptions = {}) {
    this.smol = options.smol ?? true;
  }

  private createWorker(entryPath: string): Worker {
    return new Worker(entryPath, {
      type: 'module',
      smol: this.smol,
    });
  }

  async spawnWorker(pluginId: string, entryPath: string): Promise<Worker> {
    await this.terminateWorker(pluginId);

    const worker = this.createWorker(entryPath);

    this.workers.set(pluginId, worker);
    this.entryPaths.set(pluginId, entryPath);
    return worker;
  }

  async createReloadWorker(pluginId: string, entryPath: string): Promise<Worker> {
    const activeWorker = this.workers.get(pluginId);
    if (!activeWorker) {
      throw new Error(`Cannot reload worker for plugin ${pluginId}: no active worker`);
    }

    /**
     * 重载预热阶段只允许一个候选新 Worker。
     * 这里先清理历史重载状态，避免同一个插件并发预热导致 active 指针错乱。
     */
    const existingReload = this.reloadWorkers.get(pluginId);
    if (existingReload) {
      this.reloadWorkers.delete(pluginId);
      await Promise.allSettled([
        Promise.resolve(existingReload.newWorker.terminate()),
        existingReload.active === 'new'
          ? Promise.resolve(existingReload.oldWorker.terminate())
          : Promise.resolve(undefined),
      ]);
    }

    const newWorker = this.createWorker(entryPath);
    this.reloadWorkers.set(pluginId, {
      oldWorker: activeWorker,
      newWorker,
      active: 'old',
    });
    this.entryPaths.set(pluginId, entryPath);
    return newWorker;
  }

  switchToNewWorker(pluginId: string): void {
    const reloadState = this.reloadWorkers.get(pluginId);
    if (!reloadState) {
      throw new Error(`Cannot switch worker for plugin ${pluginId}: reload worker missing`);
    }

    reloadState.active = 'new';
    this.workers.set(pluginId, reloadState.newWorker);
  }

  cancelReload(pluginId: string): void {
    const reloadState = this.reloadWorkers.get(pluginId);
    if (!reloadState) {
      return;
    }

    this.workers.set(pluginId, reloadState.oldWorker);
    this.reloadWorkers.delete(pluginId);
    void Promise.resolve(reloadState.newWorker.terminate()).catch(() => undefined);
  }

  async cleanupOldWorker(pluginId: string): Promise<void> {
    const reloadState = this.reloadWorkers.get(pluginId);
    if (!reloadState || reloadState.active !== 'new') {
      return;
    }

    /**
     * 切换后保留短暂宽限期，让旧 Worker 处理尾部消息并完成资源释放。
     * 若状态已变化（例如回滚或再次重载），则跳过清理，避免误杀当前有效实例。
     */
    await new Promise<void>((resolve) => {
      setTimeout(resolve, DEFAULT_RELOAD_GRACE_PERIOD_MS);
    });

    const latestReloadState = this.reloadWorkers.get(pluginId);
    if (latestReloadState !== reloadState || latestReloadState.active !== 'new') {
      return;
    }

    this.reloadWorkers.delete(pluginId);
    await Promise.resolve(reloadState.oldWorker.terminate()).catch(() => undefined);
  }

  async terminateWorker(pluginId: string): Promise<void> {
    const workersToTerminate = new Set<Worker>();
    const worker = this.workers.get(pluginId);
    if (worker) {
      workersToTerminate.add(worker);
    }

    const reloadState = this.reloadWorkers.get(pluginId);
    if (reloadState) {
      workersToTerminate.add(reloadState.oldWorker);
      workersToTerminate.add(reloadState.newWorker);
    }

    this.workers.delete(pluginId);
    this.reloadWorkers.delete(pluginId);

    await Promise.allSettled(
      [...workersToTerminate].map((currentWorker) => Promise.resolve(currentWorker.terminate()))
    );
  }

  async restartWorker(pluginId: string): Promise<Worker> {
    const entryPath = this.entryPaths.get(pluginId);
    if (!entryPath) {
      throw new Error(`Cannot restart worker for plugin ${pluginId}: missing entry path`);
    }

    return this.spawnWorker(pluginId, entryPath);
  }

  getWorker(pluginId: string): Worker | undefined {
    const reloadState = this.reloadWorkers.get(pluginId);
    if (reloadState) {
      return reloadState.active === 'new' ? reloadState.newWorker : reloadState.oldWorker;
    }

    return this.workers.get(pluginId);
  }

  async forgetWorker(pluginId: string): Promise<void> {
    await this.terminateWorker(pluginId);
    this.entryPaths.delete(pluginId);
  }
}

export class CircuitBreaker {
  readonly maxRestarts: number;
  readonly memoryThreshold: number;
  readonly restartCount = new Map<string, number>();

  private readonly healthByWorker = new WeakMap<Worker, PluginHealthReport>();

  constructor(options: CircuitBreakerOptions = {}) {
    this.maxRestarts = options.maxRestarts ?? DEFAULT_MAX_RESTARTS;
    this.memoryThreshold = options.memoryThreshold ?? DEFAULT_MEMORY_THRESHOLD;
  }

  updateHealth(worker: Worker, report: PluginHealthReport): void {
    this.healthByWorker.set(worker, report);
  }

  async checkMemory(worker: Worker): Promise<boolean> {
    const report = this.healthByWorker.get(worker);
    if (!report) {
      return true;
    }

    return report.memoryUsage.rss <= this.memoryThreshold;
  }

  shouldRestart(pluginId: string): boolean {
    return (this.restartCount.get(pluginId) ?? 0) < this.maxRestarts;
  }

  recordRestart(pluginId: string): void {
    const current = this.restartCount.get(pluginId) ?? 0;
    this.restartCount.set(pluginId, current + 1);
  }

  resetRestartCount(pluginId: string): void {
    this.restartCount.delete(pluginId);
  }

  getRestartCount(pluginId: string): number {
    return this.restartCount.get(pluginId) ?? 0;
  }

  getWorkerMemory(worker: Worker): number | undefined {
    return this.healthByWorker.get(worker)?.memoryUsage.rss;
  }
}

export class PluginIsolateManager {
  private readonly workerPool: WorkerPool;
  private readonly circuitBreaker: CircuitBreaker;
  private readonly monitorIntervalMs: number;

  private readonly isolates = new Map<string, IsolateRuntime>();
  private readonly workerHandlers = new WeakMap<Worker, WorkerHandlers>();
  private readonly portHandlers = new WeakMap<MessagePort, PortHandlers>();
  private readonly monitorTimers = new Map<string, ReturnType<typeof setInterval>>();
  private readonly expectedShutdown = new Set<string>();
  private readonly restartInProgress = new Set<string>();

  constructor(options: PluginIsolateManagerOptions = {}) {
    this.workerPool = options.workerPool ?? new WorkerPool({ smol: true });
    this.circuitBreaker =
      options.circuitBreaker ??
      new CircuitBreaker({
        maxRestarts: options.maxRestarts,
        memoryThreshold: options.memoryThreshold,
      });
    this.monitorIntervalMs = options.monitorIntervalMs ?? DEFAULT_MONITOR_INTERVAL_MS;
  }

  async createIsolate(
    pluginId: string,
    manifest: PluginManifest,
    entryPath: string
  ): Promise<{ worker: Worker; port: MessagePort }> {
    if (this.isolates.has(pluginId)) {
      throw new Error(`Isolate already exists for plugin ${pluginId}`);
    }

    this.circuitBreaker.resetRestartCount(pluginId);

    const worker = await this.workerPool.spawnWorker(pluginId, entryPath);
    const channel = new MessageChannel();
    const runtime: IsolateRuntime = {
      manifest,
      entryPath,
      port: channel.port1,
      startedAt: Date.now(),
    };

    this.isolates.set(pluginId, runtime);
    this.attachWorkerHandlers(pluginId, worker);
    this.attachPortHandlers(pluginId, runtime.port, worker);

    try {
      this.bootstrapWorker(pluginId, manifest, worker, channel.port2);
      this.startMemoryMonitor(pluginId);
      return { worker, port: runtime.port };
    } catch (error) {
      await this.destroyIsolate(pluginId);
      throw error;
    }
  }

  async destroyIsolate(pluginId: string): Promise<void> {
    const runtime = this.isolates.get(pluginId);
    if (!runtime) {
      return;
    }

    this.expectedShutdown.add(pluginId);

    try {
      this.stopMemoryMonitor(pluginId);

      const worker = this.workerPool.getWorker(pluginId);
      if (worker) {
        this.detachWorkerHandlers(worker);
      }

      this.detachPortHandlers(runtime.port);
      runtime.port.close();

      await this.workerPool.forgetWorker(pluginId);
      this.isolates.delete(pluginId);
      this.restartInProgress.delete(pluginId);
      this.circuitBreaker.resetRestartCount(pluginId);
    } finally {
      this.expectedShutdown.delete(pluginId);
    }
  }

  async restartIsolate(pluginId: string): Promise<Worker> {
    if (!this.circuitBreaker.shouldRestart(pluginId)) {
      throw new Error(`Restart limit reached for plugin ${pluginId}`);
    }

    this.circuitBreaker.recordRestart(pluginId);
    return this.restartIsolateUnsafe(pluginId);
  }

  isIsolateRunning(pluginId: string): boolean {
    return this.isolates.has(pluginId) && this.workerPool.getWorker(pluginId) !== undefined;
  }

  getIsolateStats(pluginId: string): IsolateStats {
    const runtime = this.isolates.get(pluginId);
    const restarts = this.circuitBreaker.getRestartCount(pluginId);

    if (!runtime) {
      return { restarts, uptime: 0 };
    }

    const uptime = Date.now() - runtime.startedAt;
    if (typeof runtime.memory === 'number') {
      return { restarts, uptime, memory: runtime.memory };
    }

    return { restarts, uptime };
  }

  private async restartIsolateUnsafe(pluginId: string): Promise<Worker> {
    const runtime = this.isolates.get(pluginId);
    if (!runtime) {
      throw new Error(`Isolate not found for plugin ${pluginId}`);
    }

    const previousWorker = this.workerPool.getWorker(pluginId);
    if (!previousWorker) {
      throw new Error(`Cannot reload isolate for plugin ${pluginId}: active worker missing`);
    }

    const previousPort = runtime.port;

    this.expectedShutdown.add(pluginId);
    this.stopMemoryMonitor(pluginId);
    this.detachWorkerHandlers(previousWorker);
    this.detachPortHandlers(previousPort);

    let reloadWorker: Worker | undefined;
    let reloadPort: MessagePort | undefined;

    try {
      reloadWorker = await this.workerPool.createReloadWorker(pluginId, runtime.entryPath);
      const channel = new MessageChannel();
      reloadPort = channel.port1;

      this.attachWorkerHandlers(pluginId, reloadWorker);
      this.attachPortHandlers(pluginId, reloadPort, reloadWorker);
      this.bootstrapWorker(pluginId, runtime.manifest, reloadWorker, channel.port2);

      /**
       * 双 Worker 重载采用先预热后切换：先让新 Worker 完成初始化，再切 active 指针。
       * 这样失败时可立即回滚到旧 Worker，成功后再异步清理旧实例，避免插件服务中断。
       */
      this.workerPool.switchToNewWorker(pluginId);

      runtime.port = reloadPort;
      runtime.startedAt = Date.now();
      runtime.memory = undefined;
      this.startMemoryMonitor(pluginId);

      previousPort.close();
      void this.workerPool.cleanupOldWorker(pluginId);

      return reloadWorker;
    } catch (error) {
      if (reloadWorker) {
        this.detachWorkerHandlers(reloadWorker);
      }

      if (reloadPort) {
        this.detachPortHandlers(reloadPort);
        reloadPort.close();
      }

      this.workerPool.cancelReload(pluginId);
      this.attachWorkerHandlers(pluginId, previousWorker);
      this.attachPortHandlers(pluginId, previousPort, previousWorker);
      this.startMemoryMonitor(pluginId);
      throw error;
    } finally {
      this.expectedShutdown.delete(pluginId);
    }
  }

  private bootstrapWorker(
    pluginId: string,
    manifest: PluginManifest,
    worker: Worker,
    workerPort: MessagePort
  ): void {
    const initMessage = createInitMessage(pluginId, manifest);
    worker.postMessage(initMessage, [workerPort]);
  }

  private attachWorkerHandlers(pluginId: string, worker: Worker): void {
    const message = (event: MessageEvent<unknown>): void => {
      this.handleIncomingMessage(pluginId, worker, event.data);
    };
    const error = (): void => {
      void this.handleUnexpectedTermination(pluginId);
    };
    const messageerror = (): void => {
      void this.handleUnexpectedTermination(pluginId);
    };

    worker.addEventListener('message', message);
    worker.addEventListener('error', error);
    worker.addEventListener('messageerror', messageerror);

    this.workerHandlers.set(worker, { message, error, messageerror });
  }

  private detachWorkerHandlers(worker: Worker): void {
    const handlers = this.workerHandlers.get(worker);
    if (!handlers) {
      return;
    }

    worker.removeEventListener('message', handlers.message);
    worker.removeEventListener('error', handlers.error);
    worker.removeEventListener('messageerror', handlers.messageerror);
    this.workerHandlers.delete(worker);
  }

  private attachPortHandlers(pluginId: string, port: MessagePort, worker: Worker): void {
    const message = (event: MessageEvent<unknown>): void => {
      this.handleIncomingMessage(pluginId, worker, event.data);
    };
    const messageerror = (): void => {
      void this.handleUnexpectedTermination(pluginId);
    };

    port.addEventListener('message', message);
    port.addEventListener('messageerror', messageerror);
    port.start();

    this.portHandlers.set(port, { message, messageerror });
  }

  private detachPortHandlers(port: MessagePort): void {
    const handlers = this.portHandlers.get(port);
    if (!handlers) {
      return;
    }

    port.removeEventListener('message', handlers.message);
    port.removeEventListener('messageerror', handlers.messageerror);
    this.portHandlers.delete(port);
  }

  private handleIncomingMessage(pluginId: string, worker: Worker, payload: unknown): void {
    if (!isPluginMessage(payload)) {
      return;
    }

    if (payload.pluginId !== pluginId || payload.type !== HEALTH_MESSAGE_TYPE) {
      return;
    }

    if (!isPluginHealthReport(payload.payload)) {
      return;
    }

    this.circuitBreaker.updateHealth(worker, payload.payload);

    const runtime = this.isolates.get(pluginId);
    if (runtime) {
      runtime.memory = payload.payload.memoryUsage.rss;
    }

    void this.checkAndHandleMemory(pluginId, worker);
  }

  private startMemoryMonitor(pluginId: string): void {
    this.stopMemoryMonitor(pluginId);

    const timer = setInterval(() => {
      const worker = this.workerPool.getWorker(pluginId);
      if (!worker) {
        return;
      }

      void this.checkAndHandleMemory(pluginId, worker);
    }, this.monitorIntervalMs);

    this.monitorTimers.set(pluginId, timer);
  }

  private stopMemoryMonitor(pluginId: string): void {
    const timer = this.monitorTimers.get(pluginId);
    if (!timer) {
      return;
    }

    clearInterval(timer);
    this.monitorTimers.delete(pluginId);
  }

  private async checkAndHandleMemory(pluginId: string, worker: Worker): Promise<void> {
    const healthy = await this.circuitBreaker.checkMemory(worker);
    if (healthy) {
      return;
    }

    void this.handleUnexpectedTermination(pluginId);
  }

  private async handleUnexpectedTermination(pluginId: string): Promise<void> {
    if (
      this.expectedShutdown.has(pluginId) ||
      this.restartInProgress.has(pluginId) ||
      !this.isolates.has(pluginId)
    ) {
      return;
    }

    if (!this.circuitBreaker.shouldRestart(pluginId)) {
      await this.destroyIsolate(pluginId);
      return;
    }

    this.restartInProgress.add(pluginId);

    try {
      this.circuitBreaker.recordRestart(pluginId);
      await this.restartIsolateUnsafe(pluginId);
    } catch {
      await this.destroyIsolate(pluginId);
    } finally {
      this.restartInProgress.delete(pluginId);
    }
  }
}

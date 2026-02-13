import type { PluginHealthReport } from '@insnh-gd/meristem-shared';

export type HealthStatus = {
  pluginId: string;
  status: 'healthy' | 'unresponsive' | 'crashed' | 'recovering';
  lastPing: number;
  lastPong: number;
  memoryUsage?: NodeJS.MemoryUsage;
  uptime: number;
  consecutiveFailures: number;
};

export type HealthMonitorOptions = {
  pingInterval?: number;
  pongTimeout?: number;
  maxConsecutiveFailures?: number;
  memoryThreshold?: number;
  restartPlugin?: (pluginId: string, reason: 'unresponsive' | 'memory-exceeded') => void;
};

const DEFAULT_PING_INTERVAL = 5_000;
const DEFAULT_PONG_TIMEOUT = 10_000;
const DEFAULT_MAX_CONSECUTIVE_FAILURES = 2;
const DEFAULT_MEMORY_THRESHOLD = 512 * 1024 * 1024;

type MonitoredPlugin = {
  worker: Worker;
  health: HealthStatus;
  messageHandler: (event: MessageEvent<unknown>) => void;
  memoryExceeded: boolean;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

const isMemoryUsage = (value: unknown): value is NodeJS.MemoryUsage => {
  if (!isRecord(value)) {
    return false;
  }

  if (
    typeof value.rss !== 'number' ||
    typeof value.heapTotal !== 'number' ||
    typeof value.heapUsed !== 'number' ||
    typeof value.external !== 'number'
  ) {
    return false;
  }

  if ('arrayBuffers' in value && typeof value.arrayBuffers !== 'number') {
    return false;
  }

  return true;
};

const isPluginHealthReport = (value: unknown): value is PluginHealthReport => {
  if (!isRecord(value)) {
    return false;
  }

  const status = value.status;
  return (
    typeof value.uptime === 'number' &&
    isMemoryUsage(value.memoryUsage) &&
    (status === 'healthy' || status === 'degraded' || status === 'unhealthy')
  );
};

const extractHealthReport = (value: unknown): PluginHealthReport | undefined => {
  if (isPluginHealthReport(value)) {
    return value;
  }

  if (!isRecord(value)) {
    return undefined;
  }

  if (isPluginHealthReport(value.report)) {
    return value.report;
  }

  if (isPluginHealthReport(value.payload)) {
    return value.payload;
  }

  return undefined;
};

const createUntrackedHealthStatus = (pluginId: string): HealthStatus => ({
  pluginId,
  status: 'unresponsive',
  lastPing: 0,
  lastPong: 0,
  uptime: 0,
  consecutiveFailures: 0,
});

const mapReportStatus = (
  previous: HealthStatus['status'],
  reportStatus: PluginHealthReport['status']
): HealthStatus['status'] => {
  if (reportStatus === 'unhealthy') {
    return 'unresponsive';
  }

  if (reportStatus === 'degraded') {
    return 'recovering';
  }

  if (previous === 'unresponsive' || previous === 'crashed') {
    return 'recovering';
  }

  if (previous === 'recovering') {
    return 'healthy';
  }

  return 'healthy';
};

const cloneHealthStatus = (health: HealthStatus): HealthStatus => ({
  ...health,
  memoryUsage: health.memoryUsage ? { ...health.memoryUsage } : undefined,
});

export class HealthMonitor {
  private readonly pingInterval: number;
  private readonly pongTimeout: number;
  private readonly maxConsecutiveFailures: number;
  private readonly memoryThreshold: number;
  private readonly restartPlugin?: (pluginId: string, reason: 'unresponsive' | 'memory-exceeded') => void;

  private readonly monitoredPlugins = new Map<string, MonitoredPlugin>();
  private readonly workerToPluginId = new WeakMap<Worker, string>();
  private monitorTimer: ReturnType<typeof setInterval> | null = null;

  constructor(options: HealthMonitorOptions = {}) {
    this.pingInterval = options.pingInterval ?? DEFAULT_PING_INTERVAL;
    this.pongTimeout = options.pongTimeout ?? DEFAULT_PONG_TIMEOUT;
    this.maxConsecutiveFailures =
      options.maxConsecutiveFailures ?? DEFAULT_MAX_CONSECUTIVE_FAILURES;
    this.memoryThreshold = options.memoryThreshold ?? DEFAULT_MEMORY_THRESHOLD;
    this.restartPlugin = options.restartPlugin;
  }

  startMonitoring(pluginId: string, worker: Worker): void {
    this.stopMonitoring(pluginId);

    const now = Date.now();
    const health: HealthStatus = {
      pluginId,
      status: 'healthy',
      lastPing: now,
      lastPong: now,
      uptime: 0,
      consecutiveFailures: 0,
    };

    const messageHandler = (event: MessageEvent<unknown>): void => {
      const report = extractHealthReport(event.data);
      if (!report) {
        return;
      }

      this.handlePong(pluginId, report);
    };

    worker.addEventListener('message', messageHandler);
    this.workerToPluginId.set(worker, pluginId);
    this.monitoredPlugins.set(pluginId, {
      worker,
      health,
      messageHandler,
      memoryExceeded: false,
    });

    this.ensureMonitoringLoop();
    this.sendPing(worker);
  }

  stopMonitoring(pluginId: string): void {
    const monitored = this.monitoredPlugins.get(pluginId);
    if (!monitored) {
      return;
    }

    monitored.worker.removeEventListener('message', monitored.messageHandler);
    this.workerToPluginId.delete(monitored.worker);
    this.monitoredPlugins.delete(pluginId);

    if (this.monitoredPlugins.size === 0) {
      this.stopMonitoringLoop();
    }
  }

  getHealth(pluginId: string): HealthStatus {
    const monitored = this.monitoredPlugins.get(pluginId);
    if (!monitored) {
      return createUntrackedHealthStatus(pluginId);
    }

    return cloneHealthStatus(monitored.health);
  }

  isResponsive(pluginId: string): boolean {
    const monitored = this.monitoredPlugins.get(pluginId);
    if (!monitored) {
      return false;
    }

    const now = Date.now();
    if (now - monitored.health.lastPong > this.pongTimeout) {
      return false;
    }

    return (
      monitored.health.status === 'healthy' || monitored.health.status === 'recovering'
    );
  }

  sendPing(worker: Worker): void {
    const pluginId = this.workerToPluginId.get(worker);
    if (!pluginId) {
      return;
    }

    const monitored = this.monitoredPlugins.get(pluginId);
    if (!monitored) {
      return;
    }

    const now = Date.now();
    monitored.health.lastPing = now;

    try {
      worker.postMessage({
        type: 'HEALTH',
        timestamp: now,
      });
    } catch {
      monitored.health.consecutiveFailures += 1;
      monitored.health.status =
        monitored.health.consecutiveFailures >= this.maxConsecutiveFailures
          ? 'crashed'
          : 'unresponsive';

      if (monitored.health.status === 'crashed') {
        this.onUnresponsive(pluginId);
      }
    }
  }

  handlePong(pluginId: string, report: PluginHealthReport): void {
    const monitored = this.monitoredPlugins.get(pluginId);
    if (!monitored) {
      return;
    }

    const previousStatus = monitored.health.status;
    monitored.health.lastPong = Date.now();
    monitored.health.memoryUsage = report.memoryUsage;
    monitored.health.uptime = report.uptime;
    monitored.health.consecutiveFailures = 0;
    monitored.health.status = mapReportStatus(previousStatus, report.status);

    const exceeded = this.checkMemoryThreshold(report);
    if (!exceeded) {
      monitored.memoryExceeded = false;
      return;
    }

    monitored.health.status = 'unresponsive';
    monitored.health.consecutiveFailures = 1;

    if (!monitored.memoryExceeded) {
      monitored.memoryExceeded = true;
      this.onMemoryExceeded(pluginId);
    }
  }

  checkDeadPlugins(): string[] {
    const now = Date.now();
    const deadPlugins: string[] = [];

    for (const [pluginId, monitored] of this.monitoredPlugins) {
      if (now - monitored.health.lastPong <= this.pongTimeout) {
        continue;
      }

      deadPlugins.push(pluginId);
      monitored.health.consecutiveFailures += 1;

      if (monitored.health.consecutiveFailures >= this.maxConsecutiveFailures) {
        if (monitored.health.status !== 'crashed') {
          monitored.health.status = 'crashed';
          this.onUnresponsive(pluginId);
        }
        continue;
      }

      monitored.health.status = 'unresponsive';
    }

    return deadPlugins;
  }

  checkMemoryThreshold(report: PluginHealthReport): boolean {
    return report.memoryUsage.rss > this.memoryThreshold;
  }

  getMemoryUsage(pluginId: string): NodeJS.MemoryUsage | undefined {
    const monitored = this.monitoredPlugins.get(pluginId);
    return monitored?.health.memoryUsage;
  }

  onUnresponsive(pluginId: string): void {
    this.restartPlugin?.(pluginId, 'unresponsive');
  }

  onMemoryExceeded(pluginId: string): void {
    this.restartPlugin?.(pluginId, 'memory-exceeded');
  }

  private ensureMonitoringLoop(): void {
    if (this.monitorTimer) {
      return;
    }

    this.monitorTimer = setInterval(() => {
      for (const monitored of this.monitoredPlugins.values()) {
        this.sendPing(monitored.worker);
      }

      this.checkDeadPlugins();
    }, this.pingInterval);
  }

  private stopMonitoringLoop(): void {
    if (!this.monitorTimer) {
      return;
    }

    clearInterval(this.monitorTimer);
    this.monitorTimer = null;
  }
}

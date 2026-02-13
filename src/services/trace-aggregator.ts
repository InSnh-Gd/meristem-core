import type { LogEnvelope, LogLevel } from '@insnh-gd/meristem-shared';
import type { JsMsg, NatsConnection } from 'nats';

const TRACE_SUBJECT = 'meristem.v1.logs.trace.>';
const TRACE_SUBJECT_PREFIX = 'meristem.v1.logs.trace.';
const DEFAULT_MAX_TRACES = 1_000;
const DEFAULT_MAX_LOGS_PER_TRACE = 100;

type TraceSubscriber = (logs: LogEnvelope[]) => void;

export type TraceAggregatorOptions = Readonly<{
  readonly maxTraces?: number;
  readonly maxLogsPerTrace?: number;
  readonly subject?: string;
}>;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isLogLevel = (value: unknown): value is LogLevel =>
  value === 'DEBUG' ||
  value === 'INFO' ||
  value === 'WARN' ||
  value === 'ERROR' ||
  value === 'FATAL';

const isLogEnvelope = (value: unknown): value is LogEnvelope => {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.ts === 'number' &&
    isLogLevel(value.level) &&
    typeof value.node_id === 'string' &&
    typeof value.source === 'string' &&
    typeof value.trace_id === 'string' &&
    typeof value.content === 'string' &&
    isRecord(value.meta)
  );
};

const normalizePositiveInt = (value: number | undefined, fallback: number): number => {
  if (value === undefined || !Number.isFinite(value) || value <= 0) {
    return fallback;
  }

  return Math.floor(value);
};

const parsePayloadLogs = (raw: string): LogEnvelope[] => {
  let parsed: unknown;

  try {
    parsed = JSON.parse(raw);
  } catch {
    return [];
  }

  if (isLogEnvelope(parsed)) {
    return [parsed];
  }

  if (!Array.isArray(parsed)) {
    return [];
  }

  const logs: LogEnvelope[] = [];
  for (const item of parsed) {
    if (isLogEnvelope(item)) {
      logs.push(item);
    }
  }
  return logs;
};

const extractTraceIdFromSubject = (subject: string): string | null => {
  if (!subject.startsWith(TRACE_SUBJECT_PREFIX)) {
    return null;
  }

  const traceId = subject.slice(TRACE_SUBJECT_PREFIX.length).trim();
  return traceId.length > 0 ? traceId : null;
};

const groupLogsByTrace = (logs: readonly LogEnvelope[]): Map<string, LogEnvelope[]> => {
  const grouped = new Map<string, LogEnvelope[]>();

  for (const log of logs) {
    const traceLogs = grouped.get(log.trace_id);
    if (traceLogs) {
      traceLogs.push(log);
      continue;
    }
    grouped.set(log.trace_id, [log]);
  }

  return grouped;
};

export class TraceAggregator {
  private readonly traces = new Map<string, LogEnvelope[]>();
  private readonly subscribers = new Map<string, Set<TraceSubscriber>>();
  private readonly textDecoder = new TextDecoder();

  private readonly maxTraces: number;
  private readonly maxLogsPerTrace: number;
  private readonly subject: string;

  private readonly connection: NatsConnection;
  private stopSubscription: (() => void) | null = null;

  public constructor(connection: NatsConnection, options: TraceAggregatorOptions = {}) {
    this.connection = connection;
    this.maxTraces = normalizePositiveInt(options.maxTraces, DEFAULT_MAX_TRACES);
    this.maxLogsPerTrace = normalizePositiveInt(
      options.maxLogsPerTrace,
      DEFAULT_MAX_LOGS_PER_TRACE,
    );
    this.subject = options.subject ?? TRACE_SUBJECT;

    this.bindNatsSubscription();
  }

  public subscribe(traceId: string, callback: (logs: LogEnvelope[]) => void): () => void {
    const normalizedTraceId = traceId.trim();
    if (normalizedTraceId.length === 0) {
      callback([]);
      return () => undefined;
    }

    const existing = this.subscribers.get(normalizedTraceId);
    const callbacks = existing ?? new Set<TraceSubscriber>();
    callbacks.add(callback);

    if (!existing) {
      this.subscribers.set(normalizedTraceId, callbacks);
    }

    callback(this.getTrace(normalizedTraceId));

    return (): void => {
      const active = this.subscribers.get(normalizedTraceId);
      if (!active) {
        return;
      }

      active.delete(callback);
      if (active.size === 0) {
        this.subscribers.delete(normalizedTraceId);
      }
    };
  }

  public aggregate(traceId: string, logs: LogEnvelope[]): void {
    const normalizedTraceId = traceId.trim();
    if (normalizedTraceId.length === 0 || logs.length === 0) {
      return;
    }

    const targetLogs = logs.filter((log) => log.trace_id === normalizedTraceId);
    if (targetLogs.length === 0) {
      return;
    }

    const existing = this.touchTrace(normalizedTraceId);
    const merged = [...existing, ...targetLogs];
    const boundedLogs =
      merged.length > this.maxLogsPerTrace
        ? merged.slice(merged.length - this.maxLogsPerTrace)
        : merged;

    this.traces.set(normalizedTraceId, boundedLogs);
    this.evictTraceLru();
    this.emit(normalizedTraceId, boundedLogs);
  }

  public getTrace(traceId: string): LogEnvelope[] {
    const normalizedTraceId = traceId.trim();
    if (normalizedTraceId.length === 0) {
      return [];
    }

    return [...this.touchTrace(normalizedTraceId)];
  }

  public clearTrace(traceId: string): void {
    const normalizedTraceId = traceId.trim();
    if (normalizedTraceId.length === 0) {
      return;
    }

    if (!this.traces.delete(normalizedTraceId)) {
      return;
    }

    this.emit(normalizedTraceId, []);
  }

  public async queryByTraceId(traceId: string): Promise<LogEnvelope[]> {
    const sorted = this.getTrace(traceId).sort((left, right) => left.ts - right.ts);
    return sorted;
  }

  public stop(): void {
    if (!this.stopSubscription) {
      return;
    }

    this.stopSubscription();
    this.stopSubscription = null;
  }

  /**
   * 逻辑块：聚合器启动时即接入 trace 日志主题。
   * - 目标：将 NATS trace 主题消息持续折叠到本地内存索引，支持低延迟查询与订阅回调。
   * - 原因：trace 调试链路对实时性敏感，采用常驻订阅避免轮询查询。
   * - 失败/降级：消息解析失败或结构不合法时直接丢弃，不影响后续消息消费。
   */
  private bindNatsSubscription(): void {
    const subscription = this.connection.subscribe(this.subject);
    this.stopSubscription = (): void => {
      subscription.unsubscribe();
    };

    void (async () => {
      for await (const message of subscription) {
        this.handleNatsMessage(message);
      }
    })();
  }

  private handleNatsMessage(message: Pick<JsMsg, 'subject' | 'data'>): void {
    const decoded = this.textDecoder.decode(message.data);
    const logs = parsePayloadLogs(decoded);
    if (logs.length === 0) {
      return;
    }

    const traceIdFromSubject = extractTraceIdFromSubject(message.subject);
    if (traceIdFromSubject) {
      const scopedLogs = logs.filter((log) => log.trace_id === traceIdFromSubject);
      if (scopedLogs.length > 0) {
        this.aggregate(traceIdFromSubject, scopedLogs);
        return;
      }
    }

    const groupedLogs = groupLogsByTrace(logs);
    for (const [traceId, traceLogs] of groupedLogs) {
      this.aggregate(traceId, traceLogs);
    }
  }

  private touchTrace(traceId: string): LogEnvelope[] {
    const existing = this.traces.get(traceId);
    if (!existing) {
      return [];
    }

    this.traces.delete(traceId);
    this.traces.set(traceId, existing);
    return existing;
  }

  /**
   * 逻辑块：trace 缓存采用 LRU 驱逐控制内存。
   * - 目标：保证最多保留 maxTraces 条 trace，避免高并发下内存线性增长。
   * - 原因：trace_id 基数不可控，必须在聚合层做硬上限。
   * - 失败/降级：超过上限时驱逐最久未访问 trace，并向订阅方发送空快照表示该 trace 已被淘汰。
   */
  private evictTraceLru(): void {
    while (this.traces.size > this.maxTraces) {
      const oldestTraceId = this.traces.keys().next().value;
      if (typeof oldestTraceId !== 'string') {
        return;
      }

      this.traces.delete(oldestTraceId);
      this.emit(oldestTraceId, []);
    }
  }

  private emit(traceId: string, logs: readonly LogEnvelope[]): void {
    const callbacks = this.subscribers.get(traceId);
    if (!callbacks || callbacks.size === 0) {
      return;
    }

    const snapshot = [...logs];
    for (const callback of callbacks) {
      try {
        callback(snapshot);
      } catch {}
    }
  }
}

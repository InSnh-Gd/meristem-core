import pino from 'pino';
import { createNatsTransport } from './nats-transport';
import { getNatsIfConnected } from '../nats/connection';
import type { TraceContext } from './trace-context.js';
import type { LogEnvelope, LogLevel } from '@insnh-gd/meristem-shared';

// Module-level singleton to avoid per-logger instances.
const sharedNatsTransport = createNatsTransport({
  getConnection: async () => {
    const connection = getNatsIfConnected();
    if (!connection) {
      throw new Error('NATS connection not available');
    }
    return connection;
  },
});

const PINO_BASE_OPTIONS = {
  level: 'debug',
  base: null,
  timestamp: false,
  formatters: {
    level: () => ({}),
    bindings: () => ({}),
  },
} as const;

type LoggerMethod = 'debug' | 'info' | 'warn' | 'error' | 'fatal';

const PINO_METHOD_TO_LEVEL: Readonly<Record<LoggerMethod, number>> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
  fatal: 50,
} as const;

const LOGGER_CACHE_MAX_SIZE = 512;

/**
 * 逻辑块：共享 stdout logger，避免每次 createLogger 都新建 transport/worker。
 * 这样可以消除高频路径中的 ThreadStream 创建成本，同时保持日志仍然输出到 stdout。
 */
const sharedStdoutDestination = pino.destination({
  dest: 1,
  sync: false,
});

const sharedPinoLogger = pino(PINO_BASE_OPTIONS, sharedStdoutDestination);

const loggerCache = new Map<string, Logger>();

/**
 * Pino log level mapping to LOG_PROTOCOL levels
 */
const PINO_LEVEL_TO_LOG_LEVEL: Readonly<Record<number, LogLevel>> = {
  10: 'DEBUG',
  20: 'INFO',
  30: 'WARN',
  40: 'ERROR',
  50: 'FATAL',
} as const;

/**
 * Type guard to check if a value is a valid LogLevel
 */
const isLogLevel = (value: unknown): value is LogLevel => {
  return (
    typeof value === 'string' &&
    (value === 'DEBUG' ||
      value === 'INFO' ||
      value === 'WARN' ||
      value === 'ERROR' ||
      value === 'FATAL')
  );
};

/**
 * Type guard to check if a value is a valid LogEnvelope
 */
const isLogEnvelope = (value: unknown): value is LogEnvelope => {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const envelope = value as Record<string, unknown>;

  return (
    typeof envelope.ts === 'number' &&
    isLogLevel(envelope.level) &&
    typeof envelope.node_id === 'string' &&
    typeof envelope.source === 'string' &&
    typeof envelope.trace_id === 'string' &&
    typeof envelope.content === 'string' &&
    typeof envelope.meta === 'object' &&
    envelope.meta !== null
  );
};

/**
 * Transform Pino log object to LOG_PROTOCOL envelope format
 */
const transformToEnvelope = (
  traceContext: TraceContext,
  data: Record<string, unknown>,
): LogEnvelope => {
  const pinoLevel = typeof data.level === 'number' ? data.level : 20;
  const logLevel = PINO_LEVEL_TO_LOG_LEVEL[pinoLevel] ?? 'INFO';
  const timestamp = typeof data.time === 'number' ? data.time : Date.now();
  const msg = typeof data.msg === 'string' ? data.msg : '';

  const { level, time, msg: _, ...rest } = data;

  const meta: Record<string, unknown> = { ...rest };

  if (traceContext.taskId) {
    meta.taskId = traceContext.taskId;
  }

  return Object.freeze({
    ts: timestamp,
    level: logLevel,
    node_id: traceContext.nodeId,
    source: traceContext.source,
    trace_id: traceContext.traceId,
    content: msg,
    meta: Object.freeze(meta),
  });
};

const buildLoggerCacheKey = (traceContext: TraceContext): string => {
  const taskId = traceContext.taskId ?? '';
  return `${traceContext.nodeId}|${traceContext.source}|${traceContext.traceId}|${taskId}`;
};

const cacheLogger = (key: string, logger: Logger): Logger => {
  if (loggerCache.size >= LOGGER_CACHE_MAX_SIZE) {
    const oldestKey = loggerCache.keys().next().value;
    if (typeof oldestKey === 'string') {
      loggerCache.delete(oldestKey);
    }
  }
  loggerCache.set(key, logger);
  return logger;
};

/**
 * Logger type - wraps Pino logger with envelope formatting
 */
export type Logger = Readonly<{
  readonly debug: (message: string, meta?: Record<string, unknown>) => void;
  readonly info: (message: string, meta?: Record<string, unknown>) => void;
  readonly warn: (message: string, meta?: Record<string, unknown>) => void;
  readonly error: (message: string, meta?: Record<string, unknown>) => void;
  readonly fatal: (message: string, meta?: Record<string, unknown>) => void;
}>;

/**
 * Create a logger instance with LOG_PROTOCOL envelope formatting
 *
 * This is a pure function that takes a TraceContext and returns a configured Logger.
 * It follows FP principles:
 * - Pure function: same input always produces same output
 * - No side effects: does not modify the input TraceContext
 * - Immutable: returns a frozen Logger object
 *
 * @param traceContext - The trace context containing node_id, source, trace_id, and optional taskId
 * @returns A Logger instance that outputs logs in LOG_PROTOCOL envelope format
 *
 * @example
 * ```ts
 * const context = createTraceContext({
 *   nodeId: 'core-001',
 *   source: 'api',
 * });
 * const logger = createLogger(context);
 * logger.info('Request received', { userId: 'user-123' });
 * ```
 */
export function createLogger(traceContext: TraceContext): Logger {
  const cached = loggerCache.get(buildLoggerCacheKey(traceContext));
  if (cached) {
    return cached;
  }

  const createLogMethod =
    (methodName: LoggerMethod) =>
    (message: string, meta: Record<string, unknown> = {}): void => {
      try {
        const envelope = transformToEnvelope(traceContext, {
          level: PINO_METHOD_TO_LEVEL[methodName],
          time: Date.now(),
          ...meta,
          msg: message,
        });
        sharedPinoLogger[methodName](envelope);

        /**
         * 逻辑块：NATS 传输写入保持“失败不阻塞业务”语义。
         * write 本身是内存缓冲写入，异常时仅吞掉错误并依赖 transport 的内部降级策略。
         */
        try {
          sharedNatsTransport.write(envelope);
        } catch {
          // Silent failure; transport handles buffering and fallback.
        }
      } catch (err: unknown) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        sharedPinoLogger.error({ msg: 'Logger error:', error: errorMessage });
      }
    };

  const logger = Object.freeze({
    debug: createLogMethod('debug'),
    info: createLogMethod('info'),
    warn: createLogMethod('warn'),
    error: createLogMethod('error'),
    fatal: createLogMethod('fatal'),
  });

  /**
   * 逻辑块：logger cache 只作为热路径复用优化。
   * 采用固定上限 + FIFO 驱逐，避免 trace_id 高基数导致无界增长。
   */
  return cacheLogger(buildLoggerCacheKey(traceContext), logger);
}

/**
 * Create a logger instance with custom transport options
 *
 * This is an advanced variant of createLogger that allows custom transport configuration.
 * Useful for testing or special logging scenarios.
 *
 * @param traceContext - The trace context
 * @param transportOptions - Custom Pino transport options
 * @returns A Logger instance with custom transport
 */
export function createLoggerWithTransport(
  traceContext: TraceContext,
  transportOptions: pino.TransportTargetOptions,
): Logger {
  const transport = pino.transport(transportOptions);

  const pinoLogger = pino(PINO_BASE_OPTIONS, transport);

  const createLogMethod =
    (methodName: LoggerMethod) =>
    (message: string, meta: Record<string, unknown> = {}): void => {
      try {
        const envelope = transformToEnvelope(traceContext, {
          level: PINO_METHOD_TO_LEVEL[methodName],
          time: Date.now(),
          ...meta,
          msg: message,
        });
        pinoLogger[methodName](envelope);
      } catch (err: unknown) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        pinoLogger.error({ msg: 'Logger error:', error: errorMessage });
      }
    };

  return Object.freeze({
    debug: createLogMethod('debug'),
    info: createLogMethod('info'),
    warn: createLogMethod('warn'),
    error: createLogMethod('error'),
    fatal: createLogMethod('fatal'),
  });
}

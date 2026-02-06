import pino from 'pino';
import { createNatsTransport } from './nats-transport';
import { getNatsIfConnected } from '../nats/connection';
import type { TraceContext } from './trace-context.js';

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

/**
 * Log levels as defined in LOG_PROTOCOL.md
 */
type LogLevel = 'DEBUG' | 'INFO' | 'WARN' | 'ERROR' | 'FATAL';

/**
 * Log envelope format as defined in LOG_PROTOCOL.md
 */
type LogEnvelope = Readonly<{
  readonly ts: number;
  readonly level: LogLevel;
  readonly node_id: string;
  readonly source: string;
  readonly trace_id: string;
  readonly content: string;
  readonly meta: Record<string, unknown>;
}>;

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
  const transport = pino.transport({
    target: 'pino/file',
    options: {
      destination: 1,
      sync: false,
    },
  });

  const pinoLogger = pino(
    {
      level: 'debug',
      base: null,
      timestamp: false,
      formatters: {
        level: () => ({}),
        bindings: () => ({}),
      },
    },
    transport,
  );

  const createLogMethod =
    (methodName: 'debug' | 'info' | 'warn' | 'error' | 'fatal') =>
    (message: string, meta: Record<string, unknown> = {}): void => {
      try {
        const pinoLevelMap: Record<typeof methodName, number> = {
          debug: 10,
          info: 20,
          warn: 30,
          error: 40,
          fatal: 50,
        };
        const envelope = transformToEnvelope(traceContext, {
          level: pinoLevelMap[methodName],
          time: Date.now(),
          ...meta,
          msg: message,
        });
        pinoLogger[methodName](envelope);

        void Promise.resolve()
          .then(() => {
            sharedNatsTransport.write(envelope);
          })
          .catch(() => {
            // Silent failure; transport handles buffering and fallback.
          });
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

  const pinoLogger = pino(
    {
      level: 'debug',
      base: null,
      timestamp: false,
      formatters: {
        level: () => ({}),
        bindings: () => ({}),
      },
    },
    transport,
  );

  const createLogMethod =
    (methodName: 'debug' | 'info' | 'warn' | 'error' | 'fatal') =>
    (message: string, meta: Record<string, unknown> = {}): void => {
      try {
        const pinoLevelMap: Record<typeof methodName, number> = {
          debug: 10,
          info: 20,
          warn: 30,
          error: 40,
          fatal: 50,
        };
        const envelope = transformToEnvelope(traceContext, {
          level: pinoLevelMap[methodName],
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

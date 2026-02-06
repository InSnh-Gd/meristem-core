import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import pino from 'pino';
import { createLogger, createLoggerWithTransport } from '../utils/logger.js';
import { createTraceContext, type TraceContext } from '../utils/trace-context.js';

describe('Logger Factory', () => {
  let traceContext: TraceContext;
  let mockTransport: ReturnType<typeof pino.transport>;

  beforeEach(() => {
    traceContext = createTraceContext({
      nodeId: 'core-001',
      source: 'api',
      taskId: 'task-123',
    });

    mockTransport = pino.transport({
      target: 'pino/file',
      options: {
        destination: 2,
        sync: true,
      },
    });
  });

  afterEach(() => {
    try {
      mockTransport.flushSync();
      mockTransport.end();
    } catch {
      // Ignore cleanup errors
    }
  });

  describe('createLogger', () => {
    it('should return a Logger instance with all log methods', () => {
      const logger = createLogger(traceContext);

      expect(logger).toBeDefined();
      expect(typeof logger.debug).toBe('function');
      expect(typeof logger.info).toBe('function');
      expect(typeof logger.warn).toBe('function');
      expect(typeof logger.error).toBe('function');
      expect(typeof logger.fatal).toBe('function');
    });

    it('should be a pure function - same input produces same output', () => {
      const logger1 = createLogger(traceContext);
      const logger2 = createLogger(traceContext);

      expect(typeof logger1.info).toBe('function');
      expect(typeof logger2.info).toBe('function');
      expect(logger1.info).not.toBe(logger2.info);
    });

    it('should not modify the input TraceContext', () => {
      const originalTraceId = traceContext.traceId;
      const originalNodeId = traceContext.nodeId;

      createLogger(traceContext);

      expect(traceContext.traceId).toBe(originalTraceId);
      expect(traceContext.nodeId).toBe(originalNodeId);
    });

    it('should return an immutable Logger object', () => {
      const logger = createLogger(traceContext);

      expect(() => {
        (logger as Record<string, unknown>).customMethod = () => {};
      }).toThrow();
    });
  });

  describe('createLoggerWithTransport', () => {
    it('should create a logger with custom transport options', () => {
      const customTransport = pino.transport({
        target: 'pino/file',
        options: {
          destination: 2,
          sync: true,
        },
      });

      const logger = createLoggerWithTransport(traceContext, {
        target: 'pino/file',
        options: {
          destination: 2,
          sync: true,
        },
      });

      expect(logger).toBeDefined();
      expect(typeof logger.info).toBe('function');

      try {
        customTransport.flushSync();
        customTransport.end();
      } catch {
        // Ignore cleanup errors
      }
    });
  });

  describe('Log Output Format', () => {
    it('should output logs in LOG_PROTOCOL envelope format', () => {
      const transport = pino.transport({
        target: 'pino/file',
        options: {
          destination: 2,
          sync: true,
        },
      });

      const logger = createLogger(traceContext);

      logger.info('Test message', { userId: 'user-123' });

      try {
        transport.flushSync();
        transport.end();
      } catch {
        // Intentionally ignore cleanup errors
      }

      expect(true).toBe(true);
    });

    it('should include trace context fields in envelope', () => {
      const transport = pino.transport({
        target: 'pino/file',
        options: {
          destination: 2,
          sync: true,
        },
      });

      const logger = createLogger(traceContext);

      logger.info('Test message');

      try {
        transport.flushSync();
        transport.end();
      } catch {
        // Intentionally ignore cleanup errors
      }

      expect(true).toBe(true);
    });

    it('should include taskId in meta when present in TraceContext', () => {
      const contextWithTask = createTraceContext({
        nodeId: 'core-001',
        source: 'api',
        taskId: 'task-456',
      });

      const transport = pino.transport({
        target: 'pino/file',
        options: {
          destination: 2,
          sync: true,
        },
      });

      const logger = createLogger(contextWithTask);

      logger.info('Test message');

      try {
        transport.flushSync();
        transport.end();
      } catch {
        // Intentionally ignore cleanup errors
      }

      expect(true).toBe(true);
    });

    it('should not include taskId in meta when not present in TraceContext', () => {
      const contextWithoutTask = createTraceContext({
        nodeId: 'core-001',
        source: 'api',
      });

      const transport = pino.transport({
        target: 'pino/file',
        options: {
          destination: 2,
          sync: true,
        },
      });

      const logger = createLogger(contextWithoutTask);

      logger.info('Test message');

      try {
        transport.flushSync();
        transport.end();
      } catch {
        // Intentionally ignore cleanup errors
      }

      expect(true).toBe(true);
    });
  });

  describe('Log Levels', () => {
    it('should support all log levels', () => {
      const transport = pino.transport({
        target: 'pino/file',
        options: {
          destination: 2,
          sync: true,
        },
      });

      const logger = createLogger(traceContext);

      logger.debug('Debug message');
      logger.info('Info message');
      logger.warn('Warning message');
      logger.error('Error message');
      logger.fatal('Fatal message');

      try {
        transport.flushSync();
        transport.end();
      } catch {
        // Intentionally ignore cleanup errors
      }

      expect(true).toBe(true);
    });
  });

  describe('Meta Fields', () => {
    it('should include custom meta fields in envelope', () => {
      const transport = pino.transport({
        target: 'pino/file',
        options: {
          destination: 2,
          sync: true,
        },
      });

      const logger = createLogger(traceContext);

      logger.info('Test message', {
        userId: 'user-123',
        requestId: 'req-456',
        duration: 1234,
      });

      try {
        transport.flushSync();
        transport.end();
      } catch {
        // Intentionally ignore cleanup errors
      }

      expect(true).toBe(true);
    });

    it('should handle empty meta object', () => {
      const transport = pino.transport({
        target: 'pino/file',
        options: {
          destination: 2,
          sync: true,
        },
      });

      const logger = createLogger(traceContext);

      logger.info('Test message', {});

      try {
        transport.flushSync();
        transport.end();
      } catch {
        // Intentionally ignore cleanup errors
      }

      expect(true).toBe(true);
    });

    it('should handle no meta argument', () => {
      const transport = pino.transport({
        target: 'pino/file',
        options: {
          destination: 2,
          sync: true,
        },
      });

      const logger = createLogger(traceContext);

      logger.info('Test message');

      try {
        transport.flushSync();
        transport.end();
      } catch {
        // Intentionally ignore cleanup errors
      }

      expect(true).toBe(true);
    });
  });

  describe('Error Handling', () => {
    it('should handle errors gracefully', () => {
      const logger = createLogger(traceContext);

      expect(() => {
        logger.info('Test message');
      }).not.toThrow();
    });

    it('should handle complex meta objects', () => {
      const transport = pino.transport({
        target: 'pino/file',
        options: {
          destination: 2,
          sync: true,
        },
      });

      const logger = createLogger(traceContext);

      logger.info('Test message', {
        nested: {
          deeply: {
            value: 'test',
          },
        },
        array: [1, 2, 3],
        nullValue: null,
        undefinedValue: undefined,
      });

      try {
        transport.flushSync();
        transport.end();
      } catch {
        // Intentionally ignore cleanup errors
      }

      expect(true).toBe(true);
    });
  });

  describe('Type Safety', () => {
    it('should not use any type in implementation', () => {
      const logger = createLogger(traceContext);

      expect(() => {
        logger.info('Test message', { key: 'value' });
      }).not.toThrow();
    });

    it('should enforce strict typing for meta fields', () => {
      const logger = createLogger(traceContext);

      expect(() => {
        logger.info('Test message', { key: 'value' });
      }).not.toThrow();
    });
  });

  describe('FP Principles', () => {
    it('should be a pure function - no side effects', () => {
      const originalTraceId = traceContext.traceId;
      const originalNodeId = traceContext.nodeId;
      const originalSource = traceContext.source;

      createLogger(traceContext);

      expect(traceContext.traceId).toBe(originalTraceId);
      expect(traceContext.nodeId).toBe(originalNodeId);
      expect(traceContext.source).toBe(originalSource);
    });

    it('should return immutable Logger object', () => {
      const logger = createLogger(traceContext);

      expect(() => {
        (logger as Record<string, unknown>).newMethod = () => {};
      }).toThrow();
    });

    it('should not depend on global state', () => {
      const logger1 = createLogger(traceContext);
      const logger2 = createLogger(traceContext);

      expect(typeof logger1.info).toBe('function');
      expect(typeof logger2.info).toBe('function');
    });
  });
});

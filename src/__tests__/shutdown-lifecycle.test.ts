import { expect, test } from 'bun:test';
import { createShutdownLifecycle } from '../runtime/shutdown-lifecycle';

type CapturedLogs = {
  info: string[];
  warn: string[];
  error: string[];
};

const createCapturedLogger = (): {
  logs: CapturedLogs;
  logger: {
    info: (message: string) => void;
    warn: (message: string) => void;
    error: (message: string) => void;
  };
} => {
  const logs: CapturedLogs = {
    info: [],
    warn: [],
    error: [],
  };
  return {
    logs,
    logger: {
      info: (message: string): void => {
        logs.info.push(message);
      },
      warn: (message: string): void => {
        logs.warn.push(message);
      },
      error: (message: string): void => {
        logs.error.push(message);
      },
    },
  };
};

test('shutdown lifecycle executes tasks in reverse registration order', async (): Promise<void> => {
  const sequence: string[] = [];
  const captured = createCapturedLogger();
  const lifecycle = createShutdownLifecycle(captured.logger, { exitOnSignal: false });

  lifecycle.addTask('first', async () => {
    sequence.push('first');
  });
  lifecycle.addTask('second', async () => {
    sequence.push('second');
  });

  await lifecycle.run('test');

  expect(sequence).toEqual(['second', 'first']);
});

test('shutdown lifecycle ignores second run after completion', async (): Promise<void> => {
  const sequence: string[] = [];
  const captured = createCapturedLogger();
  const lifecycle = createShutdownLifecycle(captured.logger, { exitOnSignal: false });

  lifecycle.addTask('single', async () => {
    sequence.push('single');
  });

  await lifecycle.run('first');
  await lifecycle.run('second');

  expect(sequence).toEqual(['single']);
});


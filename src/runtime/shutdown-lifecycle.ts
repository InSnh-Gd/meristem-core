type ShutdownTask = () => Promise<void> | void;

type ShutdownTaskItem = {
  name: string;
  run: ShutdownTask;
};

type ShutdownLogger = {
  info: (message: string) => void;
  warn: (message: string) => void;
  error: (message: string) => void;
};

export type ShutdownLifecycle = {
  addTask: (name: string, task: ShutdownTask) => void;
  run: (reason: string) => Promise<void>;
  installSignalHandlers: (signals?: readonly NodeJS.Signals[]) => void;
};

type ShutdownLifecycleOptions = {
  exitOnSignal?: boolean;
};

const DEFAULT_SIGNALS: readonly NodeJS.Signals[] = ['SIGINT', 'SIGTERM'];

const toErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

export const createShutdownLifecycle = (
  logger: ShutdownLogger,
  options: ShutdownLifecycleOptions = {},
): ShutdownLifecycle => {
  const tasks: ShutdownTaskItem[] = [];
  let running = false;
  let finished = false;
  let handlersInstalled = false;
  const exitOnSignal = options.exitOnSignal ?? true;

  const run = async (reason: string): Promise<void> => {
    if (finished) {
      return;
    }
    if (running) {
      logger.warn(`[Shutdown] already running, reason=${reason}`);
      return;
    }

    running = true;
    logger.info(`[Shutdown] begin, reason=${reason}`);
    const reversed = [...tasks].reverse();
    for (const task of reversed) {
      try {
        await task.run();
        logger.info(`[Shutdown] completed task=${task.name}`);
      } catch (error) {
        logger.error(
          `[Shutdown] task failed task=${task.name} error=${toErrorMessage(error)}`,
        );
      }
    }
    finished = true;
    running = false;
    logger.info('[Shutdown] completed all tasks');
  };

  const installSignalHandlers = (
    signals: readonly NodeJS.Signals[] = DEFAULT_SIGNALS,
  ): void => {
    if (handlersInstalled) {
      return;
    }
    handlersInstalled = true;

    for (const signal of signals) {
      process.once(signal, () => {
        void run(signal).finally(() => {
          if (exitOnSignal) {
            process.exit(0);
          }
        });
      });
    }
  };

  return {
    addTask: (name: string, task: ShutdownTask): void => {
      if (finished) {
        throw new Error('SHUTDOWN_ALREADY_FINISHED');
      }
      tasks.push({ name, run: task });
    },
    run,
    installSignalHandlers,
  };
};


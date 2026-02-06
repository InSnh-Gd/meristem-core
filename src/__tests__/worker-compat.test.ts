import pino from 'pino';

const WORKER_SCRIPT = new URL('./worker-compat.worker.ts', import.meta.url);
const WORKER_LABEL = 'Bun Worker Thread';
const TRANSPORT_LABEL = 'Pino Transport';
const WORKER_PING = 'compatibility-ping' as const;
const WORKER_PONG = 'compatibility-pong' as const;

type CompatibilityOutcome = Readonly<{
  readonly label: string;
  readonly success: boolean;
  readonly error?: string;
}>;

const isErrorLike = (value: unknown): value is { readonly message: unknown } =>
  typeof value === 'object' && value !== null && 'message' in value;

const formatError = (value: unknown): string => {
  if (typeof value === 'string') {
    return value;
  }

  if (value instanceof Error) {
    return value.message;
  }

  if (isErrorLike(value) && typeof value.message === 'string') {
    return value.message;
  }

  return 'Unknown compatibility error';
};

const createCompatibilityReport = (outcomes: readonly CompatibilityOutcome[]): string => {
  const lines = outcomes.map((outcome) => {
    const icon = outcome.success ? '✅' : '❌';
    const verdict = outcome.success ? 'COMPATIBLE' : 'INCOMPATIBLE';
    const header = `${icon} ${outcome.label}: ${verdict}`;

    if (outcome.error) {
      return `${header}\n   Error: ${outcome.error}`;
    }

    return header;
  });

  const allCompatible = outcomes.every((outcome) => outcome.success);
  const recommendation = allCompatible
    ? 'Recommendation: Use Worker Thread architecture'
    : 'Recommendation: Use main-thread batching with setInterval';
  const fallbackPlan = allCompatible
    ? ''
    : 'Fallback Plan: Buffer logs on the main thread and flush them with setInterval when workers are unavailable';

  return [...lines, recommendation, fallbackPlan].filter((line) => line.length > 0).join('\n');
};

const runWorkerCompatibility = (): Promise<CompatibilityOutcome> =>
  new Promise((resolve) => {
    let worker: Worker | null = null;
    let settled = false;

    const settle = (outcome: CompatibilityOutcome): void => {
      if (settled) {
        return;
      }

      settled = true;

      if (worker) {
        worker.removeEventListener('message', onMessage);
        worker.removeEventListener('error', onError);
        worker.terminate();
        worker = null;
      }

      resolve(outcome);
    };

    const onMessage = (event: MessageEvent<unknown>): void => {
      const payload = typeof event.data === 'string' ? event.data : String(event.data);

      if (payload === WORKER_PONG) {
        settle({ label: WORKER_LABEL, success: true });
        return;
      }

      settle({
        label: WORKER_LABEL,
        success: false,
        error: `unexpected worker response: ${payload}`,
      });
    };

    const onError = (event: ErrorEvent): void => {
      const message = isErrorLike(event.error)
        ? formatError(event.error)
        : formatError(event.message);

      settle({ label: WORKER_LABEL, success: false, error: message });
    };

    try {
      worker = new Worker(WORKER_SCRIPT, { type: 'module' });
    } catch (error) {
      settle({ label: WORKER_LABEL, success: false, error: formatError(error) });
      return;
    }

    worker.addEventListener('message', onMessage);
    worker.addEventListener('error', onError);

    try {
      worker.postMessage(WORKER_PING);
    } catch (error) {
      settle({ label: WORKER_LABEL, success: false, error: formatError(error) });
    }
  });

const runTransportCompatibility = async (): Promise<CompatibilityOutcome> => {
  const config = {
    target: 'pino/file',
    options: { destination: '/dev/null', sync: false },
  } as const;

  let transport: ReturnType<typeof pino.transport> | undefined;

  try {
    transport = pino.transport(config);
    const logger = pino(transport);
    logger.info({ compatibility: 'transport' }, 'bun transport compatibility test');
    return { label: TRANSPORT_LABEL, success: true };
  } catch (error) {
    return { label: TRANSPORT_LABEL, success: false, error: formatError(error) };
  } finally {
    if (transport) {
      try {
        transport.flushSync();
      } catch {
        // intentionally ignore compatibility flush failures
      }

      try {
        transport.end();
      } catch {
        // intentionally ignore compatibility teardown failures
      }
    }
  }
};

test('Bun worker thread and Pino transport compatibility report', async () => {
  const workerOutcome = await runWorkerCompatibility();
  const transportOutcome = await runTransportCompatibility();
  const report = createCompatibilityReport([workerOutcome, transportOutcome]);
  console.log(report);
  expect(report).toContain(WORKER_LABEL);
  expect(report).toContain(TRANSPORT_LABEL);
  expect(report).toMatch(/Recommendation:/);
});

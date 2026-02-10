import type { Msg } from 'nats';
import { Effect } from 'effect';
import { isFeatureEnabled } from '../config/feature-flags';
import { createLogger, type Logger } from '../utils/logger';
import type { TraceContext } from '../utils/trace-context';
import {
  applyBroadStrokesFilter,
  toSnapshotMeta,
  type PulseSnapshotPayload,
} from './log-triad';
import { decodeJsonBoundary, decodePulseBoundary, runBoundarySync } from './schema-boundary';

type PulseMessageLike = Pick<Msg, 'data'>;

type PulseHandlerLogger = Pick<Logger, 'info' | 'warn' | 'error'>;

type PulseIngestDeps = {
  createLogger?: (traceContext: TraceContext) => PulseHandlerLogger;
};

export const decodePulseMessage = (message: PulseMessageLike): PulseSnapshotPayload | null => {
  const fastPathEnabled = isFeatureEnabled('ENABLE_FASTPATH_HEARTBEAT');
  const program = decodeJsonBoundary(message.data, 'pulse').pipe(
    Effect.flatMap((payload) => decodePulseBoundary(payload, fastPathEnabled)),
  );
  const decoded = runBoundarySync(program);
  if (!decoded.ok) {
    return null;
  }
  return decoded.value;
};

export const createPulseMessageHandler = (deps: PulseIngestDeps = {}) => {
  const loggerFactory = deps.createLogger ?? createLogger;

  return async (traceContext: TraceContext, message: PulseMessageLike): Promise<boolean> => {
    const logger = loggerFactory(traceContext);
    const payload = decodePulseMessage(message);

    if (!payload) {
      logger.warn('[Pulse] Ignored invalid pulse payload');
      return false;
    }

    const broadMeta = applyBroadStrokesFilter(toSnapshotMeta(payload));
    logger.info('[Pulse] Snapshot ingested', {
      triad_type: 'snapshot',
      ...broadMeta,
    });
    return true;
  };
};

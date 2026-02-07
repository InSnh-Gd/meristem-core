import type { Msg } from 'nats';
import { createLogger, type Logger } from '../utils/logger';
import type { TraceContext } from '../utils/trace-context';
import {
  applyBroadStrokesFilter,
  isPulseSnapshotPayload,
  toSnapshotMeta,
  type PulseSnapshotPayload,
} from './log-triad';

type PulseMessageLike = Pick<Msg, 'data'>;

type PulseHandlerLogger = Pick<Logger, 'info' | 'warn' | 'error'>;

type PulseIngestDeps = {
  createLogger?: (traceContext: TraceContext) => PulseHandlerLogger;
};

export const decodePulseMessage = (message: PulseMessageLike): PulseSnapshotPayload | null => {
  try {
    const raw = new TextDecoder().decode(message.data);
    const parsed = JSON.parse(raw) as unknown;
    return isPulseSnapshotPayload(parsed) ? parsed : null;
  } catch {
    return null;
  }
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

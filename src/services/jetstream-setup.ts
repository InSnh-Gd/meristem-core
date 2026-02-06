import { StreamConfig, ConsumerConfig, RetentionPolicy, DiscardPolicy, AckPolicy, ReplayPolicy } from 'nats';
import { getNats } from '../nats/connection';
import { getStreamReplicas } from '../config';
import { createLogger } from '../utils/logger';
import { createTraceContext, type TraceContext } from '../utils/trace-context';

const STREAM_NAME = 'MERISTEM_LOGS';
const LOG_SUBJECTS = ['meristem.v1.logs.sys.>', 'meristem.v1.logs.task.>'];

const SEVEN_DAYS_IN_NANOSECONDS = 604800000000000n;
const TEN_GIGABYTES = 10737418240;
const TWO_MINUTES_IN_NANOSECONDS = 120000000000n;
const ONE_MEGABYTE = 1048576;

const resolveTraceContext = (traceContext?: TraceContext): TraceContext =>
  traceContext ??
  createTraceContext({
    traceId: 'system',
    nodeId: 'core',
    source: 'jetstream',
  });

const formatError = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

export async function setupJetstreamLogs(traceContext?: TraceContext): Promise<boolean> {
  const resolvedTraceContext = resolveTraceContext(traceContext);
  const logger = createLogger(resolvedTraceContext);
  try {
    const nc = await getNats(resolvedTraceContext);
    const jsm = await nc.jetstreamManager();

    const replicas = getStreamReplicas();

    const streamConfig: Partial<StreamConfig> = {
      name: STREAM_NAME,
      subjects: LOG_SUBJECTS,
      max_age: Number(SEVEN_DAYS_IN_NANOSECONDS),
      max_bytes: TEN_GIGABYTES,
      discard: DiscardPolicy.Old,
      retention: RetentionPolicy.Limits,
      num_replicas: replicas,
      duplicate_window: Number(TWO_MINUTES_IN_NANOSECONDS),
      max_msg_size: ONE_MEGABYTE,
      max_msgs: -1,
    };

    try {
      await jsm.streams.add(streamConfig);
      logger.info(`[JetStream] Created stream ${STREAM_NAME} with ${replicas} replica(s)`);
    } catch (err: unknown) {
      if (err instanceof Error && err.message.includes('stream name already in use')) {
        logger.info(`[JetStream] Stream ${STREAM_NAME} already exists, verifying configuration`);
        const info = await jsm.streams.info(STREAM_NAME);
        logger.info(
          `[JetStream] Stream config: subjects=${info.config.subjects?.join(', ')}, num_replicas=${info.config.num_replicas}`
        );
      } else {
        throw err;
      }
    }

    return true;
  } catch (error) {
    logger.warn('[JetStream] Failed to setup log stream, falling back to non-persistent mode:', {
      error: formatError(error),
    });
    return false;
  }
}

export async function createLogConsumer(
  consumerName: string = 'log-consumer',
  traceContext?: TraceContext
): Promise<ConsumerConfig | null> {
  const resolvedTraceContext = resolveTraceContext(traceContext);
  const logger = createLogger(resolvedTraceContext);
  try {
    const nc = await getNats(resolvedTraceContext);
    const jsm = await nc.jetstreamManager();

    const consumerConfig: Partial<ConsumerConfig> = {
      name: consumerName,
      durable_name: consumerName,
      ack_policy: AckPolicy.Explicit,
      max_deliver: 3,
      replay_policy: ReplayPolicy.Instant,
    };

    try {
      await jsm.consumers.add(STREAM_NAME, consumerConfig);
      logger.info(`[JetStream] Created consumer ${consumerName} on stream ${STREAM_NAME}`);
    } catch (error: unknown) {
      if (error instanceof Error && error.message.includes('consumer already exists')) {
        logger.info(`[JetStream] Consumer ${consumerName} already exists`);
      } else {
        throw error;
      }
    }

    return consumerConfig as ConsumerConfig;
  } catch (error) {
    logger.warn(`[JetStream] Failed to create consumer ${consumerName}:`, {
      error: formatError(error),
    });
    return null;
  }
}

import { StreamConfig, ConsumerConfig, RetentionPolicy, DiscardPolicy, AckPolicy, ReplayPolicy } from 'nats';
import { getNats } from '../nats/connection';
import { getStreamReplicas, getStreamMaxBytes } from '../config';
import { createLogger } from '../utils/logger';
import { createTraceContext, type TraceContext } from '../utils/trace-context';

const STREAM_NAME = 'MERISTEM_LOGS';
const LOG_SUBJECTS = ['meristem.v1.logs.sys.>', 'meristem.v1.logs.task.>', 'meristem.v1.logs.trace.>'];

const SEVEN_DAYS_IN_NANOSECONDS = 604800000000000n;
const TWO_MINUTES_IN_NANOSECONDS = 120000000000n;
const ONE_MEGABYTE = 1048576;
const STORAGE_HEADROOM_RATIO_NUMERATOR = 8;
const STORAGE_HEADROOM_RATIO_DENOMINATOR = 10;

const resolveTraceContext = (traceContext?: TraceContext): TraceContext =>
  traceContext ??
  createTraceContext({
    traceId: 'system',
    nodeId: 'core',
    source: 'jetstream',
  });

const formatError = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

type JetStreamLimits = {
  readonly max_storage: number;
  readonly storage_max_stream_bytes: number;
};

type JetStreamAccountInfoLike = {
  readonly limits: JetStreamLimits;
};

const normalizePositiveLimit = (value: number): number | null =>
  Number.isFinite(value) && value > 0 ? Math.floor(value) : null;

const resolveStreamMaxBytes = (
  configuredMaxBytes: number,
  replicas: number,
  accountInfo: JetStreamAccountInfoLike | null,
): number => {
  if (accountInfo === null) {
    return configuredMaxBytes;
  }

  const perStreamLimit = normalizePositiveLimit(accountInfo.limits.storage_max_stream_bytes);
  const accountStorageLimit = normalizePositiveLimit(accountInfo.limits.max_storage);
  const safeReplicas = Math.max(1, replicas);
  const replicaAdjustedAccountLimit =
    accountStorageLimit === null
      ? null
      : Math.floor(
          (accountStorageLimit * STORAGE_HEADROOM_RATIO_NUMERATOR) /
            STORAGE_HEADROOM_RATIO_DENOMINATOR /
            safeReplicas,
        );

  const candidates = [configuredMaxBytes];
  if (perStreamLimit !== null) {
    candidates.push(perStreamLimit);
  }
  if (replicaAdjustedAccountLimit !== null) {
    candidates.push(replicaAdjustedAccountLimit);
  }
  return Math.max(ONE_MEGABYTE, Math.min(...candidates));
};

export async function setupJetstreamLogs(traceContext?: TraceContext): Promise<boolean> {
  const resolvedTraceContext = resolveTraceContext(traceContext);
  const logger = createLogger(resolvedTraceContext);
  try {
    const nc = await getNats(resolvedTraceContext);
    const jsm = await nc.jetstreamManager();

    const replicas = getStreamReplicas();
    const configuredMaxBytes = getStreamMaxBytes();

    /**
     * 逻辑块：根据 JetStream 账户限制动态收敛 stream `max_bytes`。
     * 目标是避免“配置值看起来合法，但超出当前 NATS 账户/节点可用存储”导致启动阶段直接失败。
     * 这里优先使用明确的 per-stream 上限；其次按总存储上限扣除 20% 余量并按副本数折算。
     * 若账户信息不可用则回退到默认值，保持兼容。
     */
    let resolvedMaxBytes = configuredMaxBytes;
    try {
      const accountInfo = await jsm.getAccountInfo();
      resolvedMaxBytes = resolveStreamMaxBytes(configuredMaxBytes, replicas, accountInfo);
      if (resolvedMaxBytes !== configuredMaxBytes) {
        logger.info('[JetStream] Adjusted stream max_bytes from account limits', {
          configured_max_bytes: configuredMaxBytes,
          resolved_max_bytes: resolvedMaxBytes,
          account_max_storage: accountInfo.limits.max_storage,
          account_storage_max_stream_bytes: accountInfo.limits.storage_max_stream_bytes,
          replicas,
        });
      }
    } catch (error) {
      logger.warn('[JetStream] Failed to fetch account limits, using default stream max_bytes', {
        error: formatError(error),
        configured_max_bytes: configuredMaxBytes,
      });
    }

    const streamConfig: Partial<StreamConfig> = {
      name: STREAM_NAME,
      subjects: LOG_SUBJECTS,
      max_age: Number(SEVEN_DAYS_IN_NANOSECONDS),
      max_bytes: resolvedMaxBytes,
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

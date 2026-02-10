import { connect, NatsConnection, Subscription, Msg } from 'nats';
import { createTraceContext, type TraceContext } from '../utils/trace-context';

/**
 * NATS 连接配置
 */
type NatsConfig = {
  servers?: string;
  token?: string;
  timeout?: number;
};

type ManagedSubscription = {
  subscription: Subscription;
  [Symbol.dispose]: () => void;
};

/**
 * 解析 NATS 配置，优先使用显式参数，其次使用环境变量
 */
const resolveConfig = (override: Partial<NatsConfig> = {}): NatsConfig => {
  return {
    servers: override.servers ?? process.env.NATS_URL ?? 'nats://localhost:4222',
    token: override.token ?? process.env.NATS_TOKEN,
    timeout: override.timeout ?? 5000,
  };
};

/**
 * 模块级 NATS 连接实例（使用 let 而非 class）
 */
let nc: NatsConnection | null = null;

const resolveTraceContext = (traceContext?: TraceContext): TraceContext =>
  traceContext ??
  createTraceContext({
    traceId: 'system',
    nodeId: 'core',
    source: 'nats',
  });

const formatError = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

const getLogger = async (traceContext: TraceContext) => {
  const { createLogger } = await import('../utils/logger');
  return createLogger(traceContext);
};

/**
 * 纯函数：建立 NATS 连接
 * 多次调用返回同一实例，避免重复连接
 */
export const connectNats = async (
  traceContext: TraceContext,
  override: Partial<NatsConfig> = {}
): Promise<NatsConnection> => {
  if (nc) {
    return nc;
  }

  const logger = await getLogger(traceContext);
  const config = resolveConfig(override);
  nc = await connect({
    servers: config.servers,
    token: config.token,
    timeout: config.timeout,
  });

  logger.info(`[NATS] 已连接到 ${config.servers}`);
  return nc;
};

/**
 * 纯函数：获取当前 NATS 连接实例
 * 若尚未连接则自动建立
 */
export const getNats = async (traceContext?: TraceContext): Promise<NatsConnection> => {
  if (!nc) {
    return connectNats(resolveTraceContext(traceContext));
  }
  return nc;
};

/**
 * 获取已存在的 NATS 连接（不触发新连接）
 * 用于避免 logger → NATS → logger 循环依赖
 */
export const getNatsIfConnected = (): NatsConnection | null => {
  return nc;
};

/**
 * 纯函数：关闭 NATS 连接并清理模块级状态
 * 便于测试重置和优雅关闭
 */
export const closeNats = async (traceContext?: TraceContext): Promise<void> => {
  if (!nc) {
    return;
  }

  const logger = await getLogger(resolveTraceContext(traceContext));
  try {
    await nc.close();
    logger.info('[NATS] 连接已关闭');
  } catch (error) {
    logger.error('[NATS] 关闭连接时出错:', {
      error: formatError(error),
    });
  } finally {
    nc = null;
  }
};

/**
 * 纯函数：订阅指定主题并处理消息
 * 返回 Subscription 对象，可用于取消订阅
 */
export const subscribe = async (
  traceContext: TraceContext,
  subject: string,
  callback: (msg: Msg) => void | Promise<void>,
  queue?: string
): Promise<Subscription> => {
  const logger = await getLogger(traceContext);
  const conn = await getNats(traceContext);
  const sub = conn.subscribe(subject, { queue });
  logger.info(`[NATS] 已订阅主题: ${subject}${queue ? ` (队列: ${queue})` : ''}`);

  // 异步处理消息，避免阻塞订阅循环
  (async () => {
    for await (const msg of sub) {
      try {
        await callback(msg);
      } catch (error) {
        logger.error(`[NATS] 处理消息时出错 [${subject}]:`, {
          error: formatError(error),
        });
      }
    }
  })();

  return sub;
};

export const toManagedSubscription = (
  subscription: Subscription,
): ManagedSubscription => ({
  subscription,
  [Symbol.dispose]: (): void => {
    subscription.unsubscribe();
  },
});

export const withSubscription = async <T>(
  traceContext: TraceContext,
  subject: string,
  callback: (msg: Msg) => void | Promise<void>,
  work: (subscription: Subscription) => Promise<T>,
  queue?: string
): Promise<T> => {
  const subscription = await subscribe(traceContext, subject, callback, queue);
  using managed = toManagedSubscription(subscription);
  return work(managed.subscription);
};

/**
 * 纯函数：发布消息到指定主题
 */
export const publish = async (
  traceContext: TraceContext,
  subject: string,
  data: string | Uint8Array
): Promise<void> => {
  const conn = await getNats(traceContext);
  await conn.publish(subject, data);
};

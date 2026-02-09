import { Db } from 'mongodb';
import { Msg } from 'nats';
import { NODES_COLLECTION, type NodeDocument } from '../db/collections';
import { createLogger } from '../utils/logger';
import type { TraceContext } from '../utils/trace-context';

/**
 * 心跳消息类型定义（来自 EVENT_BUS_SPEC.md §6.2）
 */
type HeartbeatMessage = {
  node_id: string;
  ts: number;
  v: number;
  claimed_ip?: string;
};

/**
 * 心跳 ACK 响应类型（来自 EVENT_BUS_SPEC.md §6.3）
 */
type HeartbeatAck = {
  ack: true;
  sps: number;
};

/**
 * 时效性参数（来自 EVENT_BUS_SPEC.md §7）
 */
const HEARTBEAT_INTERVAL_MS = 15000; // 15s 心跳周期
const OFFLINE_THRESHOLD_MS = 45000; // 45s 离线阈值 (3 × Heartbeat)
const DEFAULT_SPS = 1280; // 默认安全负载大小（Lv2 UDP Relay）

const formatError = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

/**
 * 纯函数：更新节点心跳状态
 * 原子更新节点的 last_seen 时间戳和 online 状态
 */
export const updateNodeHeartbeat = async (
  db: Db,
  traceContext: TraceContext,
  nodeId: string,
  timestamp: Date
): Promise<void> => {
  const logger = createLogger(traceContext);
  const nodesCollection = db.collection<NodeDocument>(NODES_COLLECTION);
  const result = await nodesCollection.updateOne(
    { node_id: nodeId },
    {
      $set: {
        'status.online': true,
        'status.last_seen': timestamp,
        'status.connection_status': 'online',
      },
    }
  );

  if (result.matchedCount === 0) {
    logger.warn(`[Heartbeat] 未找到节点: ${nodeId}`);
  }
};

/**
 * 纯函数：检测离线节点
 * 查询 last_seen 超过阈值（45s）的节点，将其标记为离线
 */
export const checkNodeOffline = async (
  db: Db,
  traceContext: TraceContext,
  thresholdMs: number = OFFLINE_THRESHOLD_MS
): Promise<string[]> => {
  const logger = createLogger(traceContext);
  const threshold = new Date(Date.now() - thresholdMs);
  const nodesCollection = db.collection<NodeDocument>(NODES_COLLECTION);

  const result = await nodesCollection.updateMany(
    {
      'status.online': true,
      'status.last_seen': { $lt: threshold },
    },
    {
      $set: {
        'status.online': false,
        'status.connection_status': 'offline',
      },
    }
  );

  if (result.modifiedCount > 0) {
    logger.info(`[Heartbeat] 标记 ${result.modifiedCount} 个节点为离线`);
  }

  // 返回离线节点 ID 列表（用于日志或通知）
  const offlineNodes = await nodesCollection.find(
      {
        'status.online': false,
        'status.last_seen': { $lt: threshold },
      },
      { projection: { node_id: 1 } }
    )
    .toArray();

  return offlineNodes.map((node) => node.node_id as string);
};

/**
 * 纯函数：处理 NATS 心跳消息
 * 解析消息、更新节点状态、发送 ACK 响应
 */
export const handleHeartbeatMessage = async (
  db: Db,
  traceContext: TraceContext,
  msg: Msg
): Promise<void> => {
  const logger = createLogger(traceContext);
  try {
    // 解析心跳消息（使用明确的类型断言）
    const data = JSON.parse(msg.data.toString()) as HeartbeatMessage;

    // 验证必需字段
    if (!data.node_id || !data.ts || typeof data.v !== 'number') {
      logger.error('[Heartbeat] 无效的心跳消息格式:', { data });
      return;
    }

    // 更新节点心跳状态
    const timestamp = new Date(data.ts);
    await updateNodeHeartbeat(db, traceContext, data.node_id, timestamp);

    // 发送 ACK 响应（包含安全负载大小建议）
    const ack: HeartbeatAck = {
      ack: true,
      sps: DEFAULT_SPS,
    };

    if (msg.reply) {
      msg.respond(JSON.stringify(ack));
    }
  } catch (error) {
    logger.error('[Heartbeat] 处理心跳消息时出错:', {
      error: formatError(error),
    });
  }
};

/**
 * 模块级定时器引用（用于停止监控）
 */
let monitorTimer: ReturnType<typeof setInterval> | null = null;

/**
 * 纯函数：启动心跳监控定时器
 * 每 15s 运行一次离线检测，检测超过 45s 未收到心跳的节点
 */
export const startHeartbeatMonitor = async (
  db: Db,
  traceContext: TraceContext,
  intervalMs: number = HEARTBEAT_INTERVAL_MS
): Promise<void> => {
  const logger = createLogger(traceContext);
  if (monitorTimer) {
    logger.warn('[Heartbeat] 监控定时器已在运行');
    return;
  }

  logger.info(
    `[Heartbeat] 启动心跳监控，间隔: ${intervalMs}ms，离线阈值: ${OFFLINE_THRESHOLD_MS}ms`
  );

  monitorTimer = setInterval(async () => {
    try {
      await checkNodeOffline(db, traceContext, OFFLINE_THRESHOLD_MS);
    } catch (error) {
      logger.error('[Heartbeat] 离线检测时出错:', {
        error: formatError(error),
      });
    }
  }, intervalMs);
};

/**
 * 纯函数：停止心跳监控定时器
 */
export const stopHeartbeatMonitor = (traceContext: TraceContext): void => {
  const logger = createLogger(traceContext);
  if (monitorTimer) {
    clearInterval(monitorTimer);
    monitorTimer = null;
    logger.info('[Heartbeat] 监控定时器已停止');
  }
};

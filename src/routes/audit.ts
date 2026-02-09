import { Elysia, t } from 'elysia';
import { Db } from 'mongodb';
import { AuditLog, verifyChain, AUDIT_COLLECTION } from '../services/audit';
import { requireAuth } from '../middleware/auth';
import { requirePermission } from '../middleware/rbac';
import { normalizePagination, resolveQueryMaxTimeMs } from '../db/query-policy';

/**
 * 审计日志查询参数 Schema
 *
 * 使用 Elysia 的 t.Object() 进行运行时验证和类型推导
 * 支持按时间范围、操作者、日志级别、来源等条件过滤
 */
export const AuditLogsQuerySchema = t.Object({
  /**
   * 开始时间（ISO 8601 格式）
   * 可选，用于过滤时间范围
   */
  start: t.Optional(t.String({
    description: '开始时间（ISO 8601 格式）',
    format: 'date-time',
  })),

  /**
   * 结束时间（ISO 8601 格式）
   * 可选，用于过滤时间范围
   */
  end: t.Optional(t.String({
    description: '结束时间（ISO 8601 格式）',
    format: 'date-time',
  })),

  /**
   * 操作者（actor）
   * 可选，用于过滤特定用户的操作
   */
  actor: t.Optional(t.String({
    description: '操作者标识（user_id 或 node_id）',
  })),

  /**
   * 日志级别
   * 可选，用于过滤特定级别的日志
   */
  level: t.Optional(t.Union([
    t.Literal('DEBUG'),
    t.Literal('INFO'),
    t.Literal('WARN'),
    t.Literal('ERROR'),
    t.Literal('FATAL'),
  ], {
    description: '日志级别',
  })),

  /**
   * 来源模块
   * 可选，用于过滤特定模块的日志
   */
  source: t.Optional(t.String({
    description: '来源模块（如: m-net, worker-vm, core）',
  })),

  /**
   * 分页限制
   * 可选，默认 100，最大 1000
   */
  limit: t.Optional(t.Numeric({
    description: '分页限制（默认 100，最大 1000）',
    minimum: 1,
    maximum: 1000,
  })),

  /**
   * 分页偏移
   * 可选，默认 0
   */
  offset: t.Optional(t.Numeric({
    description: '分页偏移（默认 0）',
    minimum: 0,
  })),
});

/**
 * 审计日志查询响应 Schema
 */
export const AuditLogsResponseSchema = t.Object({
  success: t.Boolean({
    description: '请求是否成功',
  }),
  data: t.Array(t.Object({
    ts: t.Number({
      description: 'Unix 毫秒时间戳',
    }),
    level: t.Union([
      t.Literal('DEBUG'),
      t.Literal('INFO'),
      t.Literal('WARN'),
      t.Literal('ERROR'),
      t.Literal('FATAL'),
    ], {
      description: '日志级别',
    }),
    node_id: t.String({
      description: '产生日志的节点 ID',
    }),
    source: t.String({
      description: '模块名',
    }),
    trace_id: t.String({
      description: '全局链路追踪 ID',
    }),
    content: t.String({
      description: '日志正文',
    }),
    meta: t.Record(t.String(), t.Unknown(), {
      description: '元数据',
    }),
    _sequence: t.Number({
      description: '序列号',
    }),
    _hash: t.String({
      description: '当前日志的 SHA-256 哈希值',
    }),
    _previous_hash: t.String({
      description: '前一条日志的哈希值',
    }),
  }), {
    description: '审计日志列表',
  }),
  chainValid: t.Boolean({
    description: '哈希链是否完整有效',
  }),
  total: t.Number({
    description: '符合条件的日志总数',
  }),
});

const AuditLogsErrorSchema = t.Object({
  success: t.Boolean(),
  error: t.String(),
  message: t.Optional(t.String()),
});

const QUERY_MAX_TIME_MS = resolveQueryMaxTimeMs();

/**
 * 纯函数：构建 MongoDB 查询过滤器
 *
 * 根据查询参数构建 MongoDB 查询条件
 * 支持时间范围、操作者、日志级别、来源等过滤
 *
 * @param query - 审计日志查询参数
 * @returns MongoDB 查询过滤器对象
 */
const buildQueryFilter = (query: {
  start?: string;
  end?: string;
  actor?: string;
  level?: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR' | 'FATAL';
  source?: string;
}): Record<string, unknown> => {
  const filter: Record<string, unknown> = {};

  // 时间范围过滤
  if (query.start || query.end) {
    filter.ts = {};
    if (query.start) {
      const startTime = new Date(query.start).getTime();
      (filter.ts as Record<string, unknown>).$gte = startTime;
    }
    if (query.end) {
      const endTime = new Date(query.end).getTime();
      (filter.ts as Record<string, unknown>).$lte = endTime;
    }
  }

  // 操作者过滤（在 meta.actor 中查找）
  if (query.actor) {
    filter['meta.actor'] = query.actor;
  }

  // 日志级别过滤
  if (query.level) {
    filter.level = query.level;
  }

  // 来源模块过滤
  if (query.source) {
    filter.source = query.source;
  }

  return filter;
};

/**
 * 纯函数：查询审计日志
 *
 * 从数据库查询审计日志，支持多种过滤条件和分页
 * 查询结果按 _sequence 升序排列，确保哈希链顺序正确
 *
 * @param db - MongoDB 数据库实例
 * @param query - 审计日志查询参数
 * @returns 审计日志列表和总数
 */
const queryAuditLogs = async (
  db: Db,
  query: {
    start?: string;
    end?: string;
    actor?: string;
    level?: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR' | 'FATAL';
    source?: string;
    limit?: number;
    offset?: number;
  }
): Promise<{ logs: AuditLog[]; total: number }> => {
  const collection = db.collection<AuditLog>(AUDIT_COLLECTION);

  // 构建查询过滤器
  const filter = buildQueryFilter(query);

  // 设置分页参数
  const pagination = normalizePagination(
    {
      limit: query.limit,
      offset: query.offset,
    },
    {
      defaultLimit: 100,
      maxLimit: 1_000,
      maxOffset: 100_000,
    },
  );

  // 查询总数
  const total = await collection.countDocuments(filter, {
    maxTimeMS: QUERY_MAX_TIME_MS,
  });

  // 查询日志（按 _sequence 升序排列）
  const logs = await collection
    .find(filter)
    .sort({ _sequence: 1 })
    .skip(pagination.offset)
    .limit(pagination.limit)
    .maxTimeMS(QUERY_MAX_TIME_MS)
    .toArray();

  return { logs, total };
};

/**
 * 审计日志查询端点
 *
 * GET /api/v1/audit-logs
 *
 * 功能说明：
 * 1. 支持按时间范围、操作者、日志级别、来源等条件过滤
 * 2. 支持分页查询（limit/offset）
 * 3. 自动验证哈希链完整性
 * 4. 返回日志列表和验证状态
 *
 * 权限要求：
 * - 需要认证（待 Task 7 实现 requireAuth）
 * - 需要 sys:audit 权限（待 Task 7 实现 requirePermission）
 *
 * @param app - Elysia 应用实例
 * @returns 配置了审计日志查询路由的 Elysia 实例
 */
export const auditRoute = (app: Elysia, db: Db): Elysia => {
  app.get(
    '/api/v1/audit-logs',
    async ({ query }) => {
      // 查询审计日志
      const { logs, total } = await queryAuditLogs(db, query);

      // 验证哈希链完整性
      const chainVerification = verifyChain(logs);

      return {
        success: true,
        data: logs,
        chainValid: chainVerification.valid,
        total,
      };
    },
    {
      query: AuditLogsQuerySchema,
      response: {
        200: AuditLogsResponseSchema,
        401: AuditLogsErrorSchema,
        403: AuditLogsErrorSchema,
        500: AuditLogsErrorSchema,
      },
      beforeHandle: [requireAuth, requirePermission('sys:audit')],
    },
  );

  return app;
};

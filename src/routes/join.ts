import { Elysia, t } from 'elysia';
import { Db } from 'mongodb';
import { NodeDocument, NODES_COLLECTION } from '../db/collections';
import { extractTraceId, generateTraceId } from '../utils/trace-context';
import {
  logAuditEvent,
  type AuditEventInput,
  type AuditLog,
} from '../services/audit';

/**
 * Persona 类型定义：节点角色标识
 * - AGENT: 运行在 Core 宿主机上的本地 Agent
 * - GIG: 远程工作节点
 */
export type Persona = 'AGENT' | 'GIG';

/**
 * Join 请求体 Schema
 * 使用 Elysia 的 t.Object() 进行运行时验证和类型推导
 */
export const JoinRequestBodySchema = t.Object({
  hwid: t.String({
    description: '硬件唯一指纹，SHA-256(UUID + MAC)',
    minLength: 64,
    maxLength: 64,
  }),
  hostname: t.String({
    description: '节点主机名',
    minLength: 1,
    maxLength: 255,
  }),
  persona: t.Union([t.Literal('AGENT'), t.Literal('GIG')], {
    description: '节点角色标识',
  }),
});

/**
 * Join 响应体 Schema
 */
export const JoinResponseBodySchema = t.Object({
  success: t.Boolean(),
  data: t.Object({
    node_id: t.String({
      description: '节点唯一标识符',
      pattern: '^[a-z0-9-]{3,32}$',
    }),
    core_ip: t.String({
      description: 'Core 虚拟 IP 地址',
      pattern: '^10\\.25\\.',
    }),
    status: t.Union([t.Literal('new'), t.Literal('existing')], {
      description: '节点状态：new=新节点，existing=恢复身份',
    }),
  }),
});

/**
 * 纯函数：生成 HWID（硬件唯一指纹）
 * 基于 docs/standards/HARDWARE_PROTOCOL.md §2 HWID 生成规范
 *
 * 算法：SHA-256(UUID + MAC)
 * - UUID 从 /sys/class/dmi/id/product_uuid 读取
 * - MAC 地址从网络接口获取
 *
 * @param uuid - 系统产品 UUID
 * @param mac - 网络接口 MAC 地址
 * @returns SHA-256 哈希值（64 字符十六进制字符串）
 */
export const generateHWID = async (uuid: string, mac: string): Promise<string> => {
  const combined = `${uuid}${mac}`;
  const encoder = new TextEncoder();
  const data = encoder.encode(combined);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const hashHex = hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
  return hashHex;
};

/**
 * 纯函数：创建新节点
 * 自动分配 node_id，遵循正则约束 ^[a-z0-9-]{3,32}$
 *
 * @param db - MongoDB 数据库实例
 * @param hwid - 硬件唯一指纹
 * @param persona - 节点角色
 * @returns 创建的节点文档
 */
export const createNode = async (
  db: Db,
  hwid: string,
  persona: Persona,
): Promise<NodeDocument> => {
  // 生成 node_id：node-{timestamp}-{random}
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substring(2, 8);
  const node_id = `node-${timestamp}-${random}`;

  const now = new Date();

  const newNode: NodeDocument = {
    node_id,
    hwid,
    hostname: '', // 将在路由处理中从请求体填充
    persona,
    role_flags: {
      is_relay: false,
      is_storage: false,
      is_compute: false,
    },
    network: {
      virtual_ip: '',
      mode: 'DIRECT',
      v: 0,
    },
    inventory: {
      cpu_model: '',
      cores: 0,
      ram_total: 0,
      os: '',
      arch: 'x86_64',
    },
    status: {
      online: false,
      connection_status: 'pending_approval',
      last_seen: now,
      cpu_usage: 0,
      ram_free: 0,
      gpu_info: [],
    },
    created_at: now,
  };

  await db.collection<NodeDocument>(NODES_COLLECTION).insertOne(newNode);
  return newNode;
};

/**
 * 纯函数：HWID 亲和性检测
 * 查询数据库，如 HWID 存在则返回现有节点
 *
 * @param db - MongoDB 数据库实例
 * @param hwid - 硬件唯一指纹
 * @returns 现有节点文档，若不存在则返回 null
 */
export const recoverNode = async (
  db: Db,
  hwid: string,
): Promise<NodeDocument | null> => {
  const node = await db
    .collection<NodeDocument>(NODES_COLLECTION)
    .findOne({ hwid });
  return node;
};

/**
 * Zero-touch Join 端点
 * 基于 docs/specs/NETWORK_SYSTEM.md §2.2 Zero-touch Join 流程
 *
 * 流程说明：
 * 1. Client 通过 HTTPS 连接 Core 公网端点 POST /api/v1/join
 * 2. Core 执行 HWID 亲和性检测
 * 3. 若 HWID 已存在，自动标记为"恢复身份"；否则创建新节点
 * 4. 返回 JOIN_ACK 响应，包含 node_id、core_ip 和状态
 *
 * @param app - Elysia 应用实例
 * @returns 配置了 join 路由的 Elysia 实例
 */
type AuditLogger = (db: Db, event: AuditEventInput) => Promise<AuditLog>;

export const joinRoute = (app: Elysia, auditLogger: AuditLogger = logAuditEvent): Elysia => {
  app.post(
    '/api/v1/join',
    async ({ body, set, request }) => {
      const { hwid, hostname, persona } = body;

      const db = await (global as { db?: Db }).db;

      if (!db) {
        set.status = 500;
        return {
          success: false,
          error: 'DATABASE_NOT_CONNECTED',
        };
      }

      const existingNode = await recoverNode(db, hwid);

      let node_id: string;
      let status: 'new' | 'existing';

      if (existingNode) {
        node_id = existingNode.node_id;
        status = 'existing';

        await db
          .collection<NodeDocument>(NODES_COLLECTION)
          .updateOne(
            { node_id },
            {
              $set: {
                hostname,
                persona,
                'status.connection_status': 'online',
                'status.last_seen': new Date(),
              },
            },
          );
      } else {
        const newNode = await createNode(db, hwid, persona);
        node_id = newNode.node_id;
        status = 'new';

        await db
          .collection<NodeDocument>(NODES_COLLECTION)
          .updateOne(
            { node_id },
            { $set: { hostname } },
          );
      }

      const traceId = extractTraceId(request.headers) ?? generateTraceId();
      const auditEvent: AuditEventInput = {
        ts: Date.now(),
        level: 'INFO',
        node_id,
        source: 'join',
        trace_id: traceId,
        content: 'Node joined',
        meta: { persona, status },
      };

      try {
        await auditLogger(db, auditEvent);
      } catch (auditError) {
        console.error('[Audit] failed to log node join', auditError);
      }

      return {
        success: true,
        data: {
          node_id,
          core_ip: '10.25.0.1',
          status,
        },
      };
    },
    {
      body: JoinRequestBodySchema,
      response: {
        200: JoinResponseBodySchema,
        500: t.Object({
          success: t.Boolean(),
          error: t.String(),
        }),
      },
    },
  );

  return app;
};

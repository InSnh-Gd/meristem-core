import { Elysia, t } from 'elysia';
import { Db } from 'mongodb';
import type { NodePersona } from '@insnh-gd/meristem-shared';
import { NodeDocument, NODES_COLLECTION, type NodeHardwareProfile } from '../db/collections';
import { extractTraceId, generateTraceId } from '../utils/trace-context';
import { DEFAULT_ORG_ID } from '../services/bootstrap';
import { PERSONA_AGENT, PERSONA_GIG } from '../utils/persona';
import {
  logAuditEvent,
  type AuditEventInput,
  type AuditLog,
} from '../services/audit';

/**
 * Persona 类型定义：节点角色标识
 * - AGENT: 常驻节点人设，具备持久身份，可部署在任意节点（Node0 只是其中一个特例）
 * - GIG: 任务驱动的人设，通常用于短生命周期的计算执行
 */
export type Persona = NodePersona;

const HARDWARE_HASH_PATTERN = /^[a-f0-9]{64}$/;

const HardwareProfileSchema = t.Object({
  cpu: t.Optional(
    t.Object({
      model: t.String(),
      cores: t.Number({ minimum: 1 }),
      threads: t.Optional(t.Number({ minimum: 1 })),
    }),
  ),
  memory: t.Optional(
    t.Object({
      total: t.Number({ minimum: 1 }),
      available: t.Optional(t.Number({ minimum: 0 })),
      type: t.Optional(t.String()),
    }),
  ),
  storage: t.Optional(
    t.Array(
      t.Object({
        type: t.Optional(t.String()),
        size: t.Optional(t.Number({ minimum: 0 })),
        total: t.Optional(t.Number({ minimum: 0 })),
        available: t.Optional(t.Number({ minimum: 0 })),
      }),
    ),
  ),
  gpu: t.Optional(
    t.Array(
      t.Object({
        model: t.String(),
        vram: t.Optional(t.Number({ minimum: 0 })),
        memory: t.Optional(t.Number({ minimum: 0 })),
      }),
    ),
  ),
  os: t.Optional(t.String()),
  arch: t.Optional(t.Union([t.Literal('x86_64'), t.Literal('arm64'), t.Literal('unknown')])),
});

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
  persona: t.Union([t.Literal(PERSONA_AGENT), t.Literal(PERSONA_GIG)], {
    description: '节点角色标识',
  }),
  hardware_profile: t.Optional(HardwareProfileSchema),
  hardware_profile_hash: t.Optional(
    t.String({
      description: '硬件画像哈希（SHA-256 十六进制）',
      pattern: '^[a-f0-9]{64}$',
    }),
  ),
  org_id: t.Optional(
    t.String({
      description: '节点所属组织 ID，默认 org-default',
      minLength: 1,
      maxLength: 128,
    }),
  ),
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
    status: t.Union([t.Literal('new'), t.Literal('existing'), t.Literal('pending_approval')], {
      description: '节点状态：new=新节点，existing=恢复身份，pending_approval=硬件漂移待审批',
    }),
  }),
});

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const canonicalizeValue = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map((item) => canonicalizeValue(item));
  }

  if (isRecord(value)) {
    const normalized: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      const candidate = value[key];
      if (candidate !== undefined) {
        normalized[key] = canonicalizeValue(candidate);
      }
    }
    return normalized;
  }

  return value;
};

const createHardwareProfileHash = async (profile: NodeHardwareProfile): Promise<string> => {
  const canonical = canonicalizeValue(profile);
  const payload = JSON.stringify(canonical);
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(payload));
  return Array.from(new Uint8Array(digest))
    .map((value) => value.toString(16).padStart(2, '0'))
    .join('');
};

type ResolvedHardwareProfileHash = {
  hash?: string;
  mismatch: boolean;
};

const resolveIncomingHardwareProfileHash = async (
  profile: NodeHardwareProfile | undefined,
  providedHash: string | undefined,
): Promise<ResolvedHardwareProfileHash> => {
  if (!profile) {
    if (providedHash && HARDWARE_HASH_PATTERN.test(providedHash)) {
      return { hash: providedHash, mismatch: false };
    }
    return { hash: undefined, mismatch: false };
  }

  const computedHash = await createHardwareProfileHash(profile);
  if (!providedHash) {
    return { hash: computedHash, mismatch: false };
  }

  if (!HARDWARE_HASH_PATTERN.test(providedHash)) {
    return { hash: computedHash, mismatch: true };
  }

  return {
    hash: computedHash,
    mismatch: providedHash !== computedHash,
  };
};

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
  options: {
    hostname?: string;
    hardwareProfile?: NodeHardwareProfile;
    hardwareProfileHash?: string;
    orgId?: string;
  } = {},
): Promise<NodeDocument> => {
  // 生成 node_id：node-{timestamp}-{random}
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substring(2, 8);
  const node_id = `node-${timestamp}-${random}`;

  const now = new Date();

  const newNode: NodeDocument = {
    node_id,
    org_id: options.orgId ?? DEFAULT_ORG_ID,
    hwid,
    hostname: options.hostname ?? '',
    persona,
    hardware_profile: options.hardwareProfile,
    hardware_profile_hash: options.hardwareProfileHash,
    hardware_profile_drift: options.hardwareProfileHash
      ? {
          detected: false,
          baseline_hash: options.hardwareProfileHash,
          incoming_hash: options.hardwareProfileHash,
        }
      : undefined,
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
      const { hwid, hostname, persona, hardware_profile, hardware_profile_hash, org_id } = body;
      const incomingHardwareProfile = hardware_profile as NodeHardwareProfile | undefined;
      const incomingOrgId = typeof org_id === 'string' && org_id.length > 0 ? org_id : DEFAULT_ORG_ID;
      const incomingHashResolution = await resolveIncomingHardwareProfileHash(
        incomingHardwareProfile,
        hardware_profile_hash,
      );
      if (incomingHashResolution.mismatch) {
        set.status = 400;
        return {
          success: false,
          error: 'HARDWARE_PROFILE_HASH_MISMATCH',
        };
      }
      const incomingHash = incomingHashResolution.hash;

      const db = (global as { db?: Db }).db;

      if (!db) {
        set.status = 500;
        return {
          success: false,
          error: 'DATABASE_NOT_CONNECTED',
        };
      }

      const existingNode = await recoverNode(db, hwid);

      let node_id: string;
      let status: 'new' | 'existing' | 'pending_approval';
      let auditLevel: 'INFO' | 'WARN' = 'INFO';
      let auditContent = 'Node joined';
      let auditMeta: Record<string, unknown> = { persona, org_id: incomingOrgId };
      const now = new Date();

      if (existingNode) {
        node_id = existingNode.node_id;
        const baselineHash = existingNode.hardware_profile_hash;
        const driftDetected =
          typeof baselineHash === 'string' &&
          baselineHash.length > 0 &&
          baselineHash !== incomingHash;

        if (driftDetected) {
          status = 'pending_approval';
          auditLevel = 'WARN';
          auditContent = 'Node join blocked by hardware profile drift';
          auditMeta = {
            ...auditMeta,
            status,
            drift_detected: true,
            baseline_hash: baselineHash,
            incoming_hash: incomingHash,
          };

          await db
            .collection<NodeDocument>(NODES_COLLECTION)
            .updateOne(
              { node_id },
              {
                $set: {
                  hostname,
                  persona,
                  org_id: existingNode.org_id || incomingOrgId,
                  ...(incomingHardwareProfile ? { hardware_profile: incomingHardwareProfile } : {}),
                  ...(incomingHash ? { hardware_profile_hash: incomingHash } : {}),
                  hardware_profile_drift: {
                    detected: true,
                    baseline_hash: baselineHash,
                    incoming_hash: incomingHash,
                    detected_at: now,
                  },
                  'status.online': false,
                  'status.connection_status': 'pending_approval',
                  'status.last_seen': now,
                },
              },
            );
        } else {
          status = 'existing';
          const nextBaselineHash = baselineHash ?? incomingHash;
          auditMeta = {
            ...auditMeta,
            status,
          };

          await db
            .collection<NodeDocument>(NODES_COLLECTION)
            .updateOne(
              { node_id },
              {
                $set: {
                  hostname,
                  persona,
                  org_id: existingNode.org_id || incomingOrgId,
                  ...(incomingHardwareProfile ? { hardware_profile: incomingHardwareProfile } : {}),
                  ...(incomingHash ? { hardware_profile_hash: incomingHash } : {}),
                  hardware_profile_drift: {
                    detected: false,
                    baseline_hash: nextBaselineHash,
                    incoming_hash: incomingHash,
                  },
                  'status.online': true,
                  'status.connection_status': 'online',
                  'status.last_seen': now,
                },
              },
            );
        }
      } else {
        const newNode = await createNode(db, hwid, persona, {
          hostname,
          hardwareProfile: incomingHardwareProfile,
          hardwareProfileHash: incomingHash,
          orgId: incomingOrgId,
        });
        node_id = newNode.node_id;
        status = 'new';
        auditMeta = {
          ...auditMeta,
          status,
        };

      }

      const traceId = extractTraceId(request.headers) ?? generateTraceId();
      const auditEvent: AuditEventInput = {
        ts: Date.now(),
        level: auditLevel,
        node_id,
        source: 'join',
        trace_id: traceId,
        content: auditContent,
        meta: auditMeta,
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
        400: t.Object({
          success: t.Boolean(),
          error: t.String(),
        }),
        500: t.Object({
          success: t.Boolean(),
          error: t.String(),
        }),
      },
    },
  );

  return app;
};

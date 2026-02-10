import { Elysia, t } from 'elysia';
import { randomUUID } from 'crypto';
import { Db } from 'mongodb';
import { TaskPayload } from '../db/collections';
import { DomainError } from '../errors/domain-error';
import {
  CreateTaskInput,
  createTask,
  listTaskDocuments,
} from '../services/task-scheduler';
import { type AuditEventInput } from '../services/audit';
import { extractTraceId, generateTraceId } from '../utils/trace-context';
import { validateCallDepthFromHeaders } from '../utils/call-depth';
import { requireAuth, type AuthStore } from '../middleware/auth';
import { DEFAULT_ORG_ID } from '../services/bootstrap';
import { getUserById } from '../services/user';
import { respondWithCode, respondWithError } from './route-errors';
import { recordInvalidCallDepthRejection } from './route-audit';
import { runInTransaction } from '../db/transactions';
import { isDomainError } from '../errors/domain-error';
import { isAuditPipelineReady, recordAuditEvent } from '../services/audit-pipeline';

const DEFAULT_LEASE_DURATION_MS = 60 * 60 * 1000;
const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;

const TasksRequestPayloadSchema = t.Record(t.String(), t.Unknown());

export const TasksRequestBodySchema = t.Object({
  name: t.String({
    description: '任务名称，用于审计和展示',
    minLength: 1,
  }),
  payload: TasksRequestPayloadSchema,
  tags: t.Optional(
    t.Array(t.String({
      description: '用于节点匹配的标签',
    })),
  ),
});

const TaskCreatedResponseSchema = t.Object({
  success: t.Literal(true),
  task_id: t.String(),
});

const TaskErrorResponseSchema = t.Object({
  success: t.Literal(false),
  error: t.String(),
});

const TasksListQuerySchema = t.Object({
  limit: t.Optional(t.Numeric({ minimum: 1, maximum: 500 })),
  cursor: t.Optional(t.String({ minLength: 1 })),
});

const TasksListItemSchema = t.Object({
  task_id: t.String(),
  owner_id: t.String(),
  org_id: t.String(),
  target_node_id: t.String(),
  status: t.String(),
  availability: t.String(),
  created_at: t.String(),
});

const TasksListResponseSchema = t.Object({
  success: t.Literal(true),
  data: t.Array(TasksListItemSchema),
  page_info: t.Object({
    has_next: t.Boolean(),
    next_cursor: t.Union([t.String(), t.Null()]),
  }),
});

type RawTaskPayload = Record<string, unknown>;
type TaskRouteUser = AuthStore['user'];
type UnauthorizedResponse = {
  success: false;
  error: string;
};
type ResolvedTaskRouteUser =
  | {
      ok: true;
      user: TaskRouteUser;
    }
  | {
      ok: false;
      denied: UnauthorizedResponse;
    };

const normalizePayload = (
  rawPayload: RawTaskPayload,
  name: string,
  tags: string[],
): { payload: TaskPayload; targetNodeId?: string } => {
  const {
    plugin_id,
    action,
    params,
    volatile,
    target_node_id,
    ...extraFields
  } = rawPayload;

  const paramsObject =
    params && typeof params === 'object' && !Array.isArray(params)
      ? (params as Record<string, unknown>)
      : {};

  const mergedParams: Record<string, unknown> = {
    ...extraFields,
    ...paramsObject,
  };

  if (tags.length) {
    mergedParams.tags = tags;
  }

  const payload: TaskPayload = {
    plugin_id: typeof plugin_id === 'string' ? plugin_id : name,
    action: typeof action === 'string' ? action : name,
    params: mergedParams,
    volatile: typeof volatile === 'boolean' ? volatile : false,
  };

  return {
    payload,
    targetNodeId: typeof target_node_id === 'string' ? target_node_id : undefined,
  };
};

const resolveRequestUser = (
  set: { status?: unknown },
  store: Record<string, unknown>,
): ResolvedTaskRouteUser => {
  /**
   * 逻辑块：路由层 user 解析保持“显式校验 + 显式拒绝”。
   * 不依赖 `store as AuthStore` 的强断言假设，避免中间件缺失/异常时出现隐式 undefined 访问。
   * 解析失败统一返回 `UNAUTHORIZED`，保证响应语义稳定。
   */
  const authStore = store as Partial<Pick<AuthStore, 'user'>>;
  if (!authStore.user) {
    return {
      ok: false,
      denied: respondWithCode(set, 'UNAUTHORIZED'),
    };
  }
  return {
    ok: true,
    user: authStore.user,
  };
};

const isSuperadmin = (user: TaskRouteUser): boolean => user.permissions.includes('*');

const resolveActorOrgId = async (
  db: Db,
  user: TaskRouteUser,
): Promise<string | null> => {
  if (user.type !== 'USER') {
    return DEFAULT_ORG_ID;
  }
  const actor = await getUserById(db, user.id);
  if (!actor) {
    return null;
  }
  return actor.org_id;
};

export const tasksRoute = (app: Elysia, db: Db): Elysia => {
  app.get(
    '/api/v1/tasks',
    async ({ query, set, store }) => {
      const resolved = resolveRequestUser(set, store);
      if (!resolved.ok) {
        return resolved.denied;
      }
      const user = resolved.user;

      const limit = query.limit ?? 100;
      const filter: Record<string, unknown> = {};

      if (!isSuperadmin(user)) {
        const actorOrgId = await resolveActorOrgId(db, user);
        if (!actorOrgId) {
          return respondWithCode(set, 'UNAUTHORIZED');
        }
        filter.org_id = actorOrgId;
      }

      try {
        const { data: tasks, page_info } = await listTaskDocuments(db, {
          filter,
          limit,
          cursor: query.cursor,
        });

        return {
          success: true,
          data: tasks.map((task) => ({
            task_id: task.task_id,
            owner_id: task.owner_id,
            org_id: task.org_id,
            target_node_id: task.target_node_id,
            status: task.status.type,
            availability: task.availability,
            created_at: task.created_at.toISOString(),
          })),
          page_info,
        };
      } catch (error) {
        return respondWithError(set, error);
      }
    },
    {
      query: TasksListQuerySchema,
      response: {
        200: TasksListResponseSchema,
        401: TaskErrorResponseSchema,
        400: TaskErrorResponseSchema,
        500: TaskErrorResponseSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  app.post(
    '/api/v1/tasks',
    async ({ body, request, set, store }) => {
      const resolved = resolveRequestUser(set, store);
      if (!resolved.ok) {
        return resolved.denied;
      }
      const user = resolved.user;

      const traceId = extractTraceId(request.headers) ?? generateTraceId();
      const actorOrgId = await resolveActorOrgId(db, user);
      if (!actorOrgId) {
        return respondWithCode(set, 'UNAUTHORIZED');
      }
      const callDepth = validateCallDepthFromHeaders(request.headers);
      if (!callDepth.ok) {
        await recordInvalidCallDepthRejection({
          db,
          routeTag: 'tasks',
          source: 'tasks',
          nodeId: user.id,
          traceId,
          reason: callDepth.reason,
          rawCallDepth: callDepth.raw ?? '',
          content: 'Rejected task request due to invalid call_depth',
        });

        return respondWithCode(set, 'INVALID_CALL_DEPTH');
      }

      const taskId = randomUUID();
      const tags = body.tags ?? [];
      const { payload, targetNodeId } = normalizePayload(body.payload, body.name, tags);

      const taskData: CreateTaskInput = {
        task_id: taskId,
        owner_id: user.id,
        org_id: actorOrgId,
        trace_id: traceId,
        target_node_id: targetNodeId ?? '',
        type: 'COMMAND',
        status: { type: 'PENDING' },
        availability: 'READY',
        payload,
        lease: {
          expire_at: new Date(Date.now() + DEFAULT_LEASE_DURATION_MS),
          heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL_MS,
        },
        progress: {
          percent: 0,
          last_log_snippet: '',
          updated_at: new Date(),
        },
        result_uri: '',
        handshake: {
          result_sent: false,
          core_acked: false,
        },
      };

      const auditEvent: AuditEventInput = {
        ts: Date.now(),
        level: 'INFO',
        node_id: user.id,
        source: 'tasks',
        trace_id: traceId,
        content: `Task created (${body.name})`,
        meta: {
          task_id: taskId,
          name: body.name,
          target_node_id: targetNodeId ?? '',
          tags,
          call_depth: callDepth.depth,
        },
      };

      try {
        if (isAuditPipelineReady()) {
          await runInTransaction(db, async (session) => {
            await createTask(db, taskData, session);
            await recordAuditEvent(db, auditEvent, { routeTag: 'tasks', session });
          });
        } else {
          await createTask(db, taskData);
          await recordAuditEvent(db, auditEvent, { routeTag: 'tasks' });
        }
      } catch (error) {
        if (isDomainError(error) && error.code === 'AUDIT_BACKPRESSURE') {
          set.headers['Retry-After'] = '1';
          return respondWithCode(set, 'AUDIT_BACKPRESSURE');
        }
        console.error('[Tasks] failed to persist task or audit intent', error);
        return respondWithError(
          set,
          new DomainError('TASK_CREATION_FAILED', { cause: error }),
        );
      }

      set.status = 201;
      return {
        success: true,
        task_id: taskId,
      };
    },
    {
      body: TasksRequestBodySchema,
      response: {
        201: TaskCreatedResponseSchema,
        400: TaskErrorResponseSchema,
        401: TaskErrorResponseSchema,
        503: TaskErrorResponseSchema,
        500: TaskErrorResponseSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  return app;
};

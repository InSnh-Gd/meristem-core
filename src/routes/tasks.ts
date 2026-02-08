import { Elysia, t } from 'elysia';
import { randomUUID } from 'crypto';
import { Db } from 'mongodb';
import { TASKS_COLLECTION, TaskDocument, TaskPayload } from '../db/collections';
import { CreateTaskInput, createTask } from '../services/task-scheduler';
import { logAuditEvent, type AuditEventInput } from '../services/audit';
import { extractTraceId, generateTraceId } from '../utils/trace-context';
import { validateCallDepthFromHeaders } from '../utils/call-depth';
import { requireAuth, type AuthStore } from '../middleware/auth';
import { DEFAULT_ORG_ID } from '../services/bootstrap';
import { getUserById } from '../services/user';

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
  offset: t.Optional(t.Numeric({ minimum: 0 })),
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
  total: t.Number(),
});

type RawTaskPayload = Record<string, unknown>;

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

const isSuperadmin = (authStore: AuthStore): boolean => authStore.user.permissions.includes('*');

const resolveActorOrgId = async (
  db: Db,
  authStore: AuthStore,
): Promise<string | null> => {
  if (authStore.user.type !== 'USER') {
    return DEFAULT_ORG_ID;
  }
  const user = await getUserById(db, authStore.user.id);
  if (!user) {
    return null;
  }
  return user.org_id;
};

export const tasksRoute = (app: Elysia, db: Db): Elysia => {
  app.get(
    '/api/v1/tasks',
    async ({ query, set, store }) => {
      const authStore = store as AuthStore;
      if (!authStore.user) {
        set.status = 401;
        return {
          success: false,
          error: 'UNAUTHORIZED',
        };
      }

      const limit = query.limit ?? 100;
      const offset = query.offset ?? 0;
      const filter: Record<string, unknown> = {};

      if (!isSuperadmin(authStore)) {
        const actorOrgId = await resolveActorOrgId(db, authStore);
        if (!actorOrgId) {
          set.status = 401;
          return {
            success: false,
            error: 'UNAUTHORIZED',
          };
        }
        filter.org_id = actorOrgId;
      }

      const collection = db.collection<TaskDocument>(TASKS_COLLECTION);
      const total = await collection.countDocuments(filter);
      const tasks = await collection
        .find(filter)
        .sort({ created_at: 1 })
        .skip(offset)
        .limit(limit)
        .toArray();

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
        total,
      };
    },
    {
      query: TasksListQuerySchema,
      response: {
        200: TasksListResponseSchema,
        401: TaskErrorResponseSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  app.post(
    '/api/v1/tasks',
    async ({ body, request, set, store }) => {
      const authStore = store as AuthStore;

      if (!authStore.user) {
        set.status = 401;
        return {
          success: false,
          error: 'UNAUTHORIZED',
        };
      }

      const traceId = extractTraceId(request.headers) ?? generateTraceId();
      const actorOrgId = await resolveActorOrgId(db, authStore);
      if (!actorOrgId) {
        set.status = 401;
        return {
          success: false,
          error: 'UNAUTHORIZED',
        };
      }
      const callDepth = validateCallDepthFromHeaders(request.headers);
      if (!callDepth.ok) {
        const invalidDepthAudit: AuditEventInput = {
          ts: Date.now(),
          level: 'WARN',
          node_id: authStore.user.id,
          source: 'tasks',
          trace_id: traceId,
          content: 'Rejected task request due to invalid call_depth',
          meta: {
            reason: callDepth.reason,
            raw_call_depth: callDepth.raw ?? '',
          },
        };

        try {
          await logAuditEvent(db, invalidDepthAudit);
        } catch (auditError) {
          console.error('[Audit] failed to log invalid call_depth rejection', auditError);
        }

        set.status = 400;
        return {
          success: false,
          error: 'INVALID_CALL_DEPTH',
        };
      }

      const taskId = randomUUID();
      const tags = body.tags ?? [];
      const { payload, targetNodeId } = normalizePayload(body.payload, body.name, tags);

      const taskData: CreateTaskInput = {
        task_id: taskId,
        owner_id: authStore.user.id,
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

      try {
        await createTask(db, taskData);
      } catch (error) {
        console.error('[Tasks] failed to persist task', error);
        set.status = 500;
        return {
          success: false,
          error: 'TASK_CREATION_FAILED',
        };
      }

      const auditEvent: AuditEventInput = {
        ts: Date.now(),
        level: 'INFO',
        node_id: authStore.user.id,
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
        await logAuditEvent(db, auditEvent);
      } catch (auditError) {
        console.error('[Audit] failed to log task creation', auditError);
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
        500: TaskErrorResponseSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  return app;
};

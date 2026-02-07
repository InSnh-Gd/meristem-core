import { Elysia, t } from 'elysia';
import { randomUUID } from 'crypto';
import { Db } from 'mongodb';
import { TaskPayload } from '../db/collections';
import { CreateTaskInput, createTask } from '../services/task-scheduler';
import { logAuditEvent, type AuditEventInput } from '../services/audit';
import { extractTraceId, generateTraceId } from '../utils/trace-context';
import { validateCallDepthFromHeaders } from '../utils/call-depth';
import { requireAuth, type AuthStore } from '../middleware/auth';

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

export const tasksRoute = (app: Elysia): Elysia => {
  app.post(
    '/api/v1/tasks',
    async ({ body, request, set, store }) => {
      const db = await (global as { db?: Db }).db;

      if (!db) {
        set.status = 500;
        return {
          success: false,
          error: 'DATABASE_NOT_CONNECTED',
        };
      }

      const authStore = store as AuthStore;

      if (!authStore.user) {
        set.status = 401;
        return {
          success: false,
          error: 'UNAUTHORIZED',
        };
      }

      const traceId = extractTraceId(request.headers) ?? generateTraceId();
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

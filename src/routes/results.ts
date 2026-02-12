import { Elysia, t } from 'elysia';
import { Db } from 'mongodb';

import { submitResult, type TaskResultPayload, type TaskResultStatus } from '../services/result-handler';
import { TASKS_COLLECTION } from '../db/collections';
import { type AuditEventInput } from '../services/audit';
import { extractTraceId, generateTraceId } from '../utils/trace-context';
import { validateCallDepthFromHeaders } from '../utils/call-depth';
import { DomainError } from '../errors/domain-error';
import { respondWithCode, respondWithError } from './route-errors';
import { recordInvalidCallDepthRejection } from './route-audit';
import { runInTransaction, type DbSession } from '../db/transactions';
import { isDomainError } from '../errors/domain-error';
import { isAuditPipelineReady, recordAuditEvent } from '../services/audit-pipeline';

const ResultsRequestSchema = t.Object({
  task_id: t.String({
    description: '任务唯一标识符',
  }),
  delivery_id: t.Optional(t.String({
    description: '结果投递幂等键（SHA-256）',
    pattern: '^[a-f0-9]{64}$',
  })),
  status: t.Union([
    t.Literal('completed'),
    t.Literal('failed'),
  ] as const, {
    description: '任务执行结果',
  }),
  output: t.Optional(t.String({
    description: '成功结果输出 URI 或摘要',
  })),
  error: t.Optional(t.String({
    description: '失败时的错误消息',
  })),
});

const ResultSuccessSchema = t.Object({
  success: t.Literal(true),
  ack: t.Literal(true),
  idempotent: t.Boolean(),
});

const ResultNotFoundSchema = t.Object({
  success: t.Literal(false),
  error: t.Literal('TASK_NOT_FOUND'),
});

const GenericErrorSchema = t.Object({
  success: t.Boolean(),
  error: t.String(),
});

type TaskAckSnapshot = Readonly<{
  taskId: string;
  targetNodeId: string;
  statusType: string;
  resultUri: string;
  resultError?: string;
  coreAcked: boolean;
  deliveryId?: string;
}>;

type ResultSubmissionOutcome =
  | Readonly<{ kind: 'missing' }>
  | Readonly<{ kind: 'idempotent'; task: TaskAckSnapshot }>
  | Readonly<{ kind: 'submitted'; task: TaskAckSnapshot }>;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const toTaskAckSnapshot = (value: unknown): TaskAckSnapshot | null => {
  if (!isRecord(value)) {
    return null;
  }

  const taskId = value.task_id;
  if (typeof taskId !== 'string' || taskId.length === 0) {
    return null;
  }

  const status = value.status;
  const statusType =
    isRecord(status) && typeof status.type === 'string' ? status.type : '';

  const handshake = value.handshake;
  const coreAcked =
    isRecord(handshake) && typeof handshake.core_acked === 'boolean'
      ? handshake.core_acked
      : false;
  const deliveryId =
    isRecord(handshake) && typeof handshake.delivery_id === 'string'
      ? handshake.delivery_id
      : undefined;

  return Object.freeze({
    taskId,
    targetNodeId:
      typeof value.target_node_id === 'string' ? value.target_node_id : 'core',
    statusType,
    resultUri: typeof value.result_uri === 'string' ? value.result_uri : '',
    resultError:
      typeof value.result_error === 'string' ? value.result_error : undefined,
    coreAcked,
    deliveryId,
  });
};

const createResultSnapshot = (
  task: Readonly<Record<string, unknown>>,
): TaskAckSnapshot | null => toTaskAckSnapshot(task);

const createExpectedStatusType = (payload: TaskResultPayload): string =>
  payload.status === 'completed' ? 'FINISHED' : 'FAILED';

const createExpectedResultUri = (payload: TaskResultPayload): string =>
  payload.status === 'completed' ? payload.output ?? '' : '';

const createExpectedResultError = (payload: TaskResultPayload): string | undefined =>
  payload.status === 'failed' ? payload.error ?? 'UNSPECIFIED_ERROR' : undefined;

const isPayloadEquivalentToSnapshot = (
  snapshot: TaskAckSnapshot,
  payload: TaskResultPayload,
): boolean => {
  if (snapshot.statusType !== createExpectedStatusType(payload)) {
    return false;
  }

  if (snapshot.resultUri !== createExpectedResultUri(payload)) {
    return false;
  }

  return snapshot.resultError === createExpectedResultError(payload);
};

const isIdempotentResubmission = (
  snapshot: TaskAckSnapshot,
  payload: TaskResultPayload,
  deliveryId: string | undefined,
): boolean => {
  if (!snapshot.coreAcked) {
    return false;
  }

  /**
   * 逻辑块：优先使用 delivery_id 判定重复投递，其次回退到结果内容等价判定。
   * 这样既能覆盖新协议的强幂等，也能兼容旧客户端未携带 delivery_id 的重试请求。
   */
  if (deliveryId && snapshot.deliveryId) {
    return deliveryId === snapshot.deliveryId;
  }

  return isPayloadEquivalentToSnapshot(snapshot, payload);
};

const readTaskSnapshot = async (
  db: Db,
  taskId: string,
  session: DbSession = null,
): Promise<TaskAckSnapshot | null> => {
  const collection = db.collection<Record<string, unknown>>(TASKS_COLLECTION);
  const rawTask = await collection.findOne(
    { task_id: taskId },
    session ? { session } : {},
  );
  if (!rawTask) {
    return null;
  }
  return createResultSnapshot(rawTask);
};

const markTaskResultAcked = async (
  db: Db,
  taskId: string,
  deliveryId: string | undefined,
  session: DbSession = null,
): Promise<void> => {
  const collection = db.collection<Record<string, unknown>>(TASKS_COLLECTION);
  const setPayload: Record<string, unknown> = {
    'handshake.result_sent': true,
    'handshake.core_acked': true,
    'handshake.acked_at': new Date().toISOString(),
  };

  if (deliveryId) {
    setPayload['handshake.delivery_id'] = deliveryId;
  }

  await collection.updateOne(
    { task_id: taskId },
    { $set: setPayload },
    session ? { session } : {},
  );
};

export const resultsRoute = (app: Elysia, db: Db): Elysia => {
  app.post(
    '/api/v1/results',
    async ({ body, request, set }) => {
      const traceId = extractTraceId(request.headers) ?? generateTraceId();
      const callDepth = validateCallDepthFromHeaders(request.headers);
      if (!callDepth.ok) {
        await recordInvalidCallDepthRejection({
          db,
          routeTag: 'results',
          source: 'results',
          nodeId: 'core',
          traceId,
          reason: callDepth.reason,
          rawCallDepth: callDepth.raw ?? '',
          content: 'Rejected result request due to invalid call_depth',
        });

        return respondWithCode(set, 'INVALID_CALL_DEPTH');
      }

      const payload: TaskResultPayload = {
        status: body.status as TaskResultStatus,
        output: body.output,
        error: body.error,
      };
      const deliveryId = body.delivery_id;

      let outcome: ResultSubmissionOutcome;
      try {
        if (isAuditPipelineReady()) {
          outcome = await runInTransaction(db, async (session) => {
            const snapshotBefore = await readTaskSnapshot(db, body.task_id, session);
            if (!snapshotBefore) {
              return Object.freeze({ kind: 'missing' as const });
            }

            if (isIdempotentResubmission(snapshotBefore, payload, deliveryId)) {
              return Object.freeze({
                kind: 'idempotent' as const,
                task: snapshotBefore,
              });
            }

            const updatedTask = await submitResult(db, body.task_id, payload, session);
            if (!updatedTask) {
              return Object.freeze({ kind: 'missing' as const });
            }

            await markTaskResultAcked(db, body.task_id, deliveryId, session);

            const snapshotAfter = await readTaskSnapshot(db, body.task_id, session);
            if (!snapshotAfter) {
              return Object.freeze({ kind: 'missing' as const });
            }

            const meta: Record<string, unknown> = {
              task_id: body.task_id,
              status: payload.status,
              result_uri: updatedTask.result_uri,
              call_depth: callDepth.depth,
            };
            if (updatedTask.result_error) {
              meta.result_error = updatedTask.result_error;
            }

            const auditEvent: AuditEventInput = {
              ts: Date.now(),
              level: 'INFO',
              node_id: updatedTask.target_node_id || 'core',
              source: 'results',
              trace_id: traceId,
              content: `Task result submitted (${payload.status})`,
              meta,
            };
            await recordAuditEvent(db, auditEvent, {
              routeTag: 'results',
              session,
            });
            return Object.freeze({
              kind: 'submitted' as const,
              task: snapshotAfter,
            });
          });
        } else {
          const snapshotBefore = await readTaskSnapshot(db, body.task_id);
          if (!snapshotBefore) {
            outcome = Object.freeze({ kind: 'missing' });
          } else if (isIdempotentResubmission(snapshotBefore, payload, deliveryId)) {
            outcome = Object.freeze({
              kind: 'idempotent',
              task: snapshotBefore,
            });
          } else {
            const updatedTask = await submitResult(db, body.task_id, payload);
            if (!updatedTask) {
              outcome = Object.freeze({ kind: 'missing' });
            } else {
              await markTaskResultAcked(db, body.task_id, deliveryId);
              const snapshotAfter = await readTaskSnapshot(db, body.task_id);
              if (!snapshotAfter) {
                outcome = Object.freeze({ kind: 'missing' });
              } else {
                outcome = Object.freeze({
                  kind: 'submitted',
                  task: snapshotAfter,
                });
              }
            }
          }
        }
      } catch (error) {
        if (isDomainError(error) && error.code === 'AUDIT_BACKPRESSURE') {
          set.headers['Retry-After'] = '1';
          return respondWithCode(set, 'AUDIT_BACKPRESSURE');
        }
        console.error('[Results] failed to persist task result', error);
        return respondWithError(
          set,
          new DomainError('RESULT_SUBMISSION_FAILED', { cause: error }),
        );
      }

      if (outcome.kind === 'missing') {
        return respondWithCode(set, 'TASK_NOT_FOUND');
      }

      if (outcome.kind === 'idempotent') {
        return {
          success: true,
          ack: true,
          idempotent: true,
        };
      }

      const meta: Record<string, unknown> = {
        task_id: body.task_id,
        status: payload.status,
        result_uri: outcome.task.resultUri,
        call_depth: callDepth.depth,
      };

      if (outcome.task.resultError) {
        meta.result_error = outcome.task.resultError;
      }

      if (!isAuditPipelineReady()) {
        const auditEvent: AuditEventInput = {
          ts: Date.now(),
          level: 'INFO',
          node_id: outcome.task.targetNodeId || 'core',
          source: 'results',
          trace_id: traceId,
          content: `Task result submitted (${payload.status})`,
          meta,
        };

        try {
          await recordAuditEvent(db, auditEvent, { routeTag: 'results' });
        } catch (auditError) {
          console.error('[Audit] failed to log task result', auditError);
        }
      }

      return {
        success: true,
        ack: true,
        idempotent: false,
      };
    },
    {
      body: ResultsRequestSchema,
      response: {
        200: ResultSuccessSchema,
        400: GenericErrorSchema,
        404: ResultNotFoundSchema,
        503: GenericErrorSchema,
        500: GenericErrorSchema,
      },
    },
  );

  return app;
};

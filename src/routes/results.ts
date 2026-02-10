import { Elysia, t } from 'elysia';
import { Db } from 'mongodb';

import { submitResult, type TaskResultPayload, type TaskResultStatus } from '../services/result-handler';
import { type AuditEventInput } from '../services/audit';
import { extractTraceId, generateTraceId } from '../utils/trace-context';
import { validateCallDepthFromHeaders } from '../utils/call-depth';
import { DomainError } from '../errors/domain-error';
import { respondWithCode, respondWithError } from './route-errors';
import { runInTransaction } from '../db/transactions';
import { isDomainError } from '../errors/domain-error';
import { isAuditPipelineReady, recordAuditEvent } from '../services/audit-pipeline';

const ResultsRequestSchema = t.Object({
  task_id: t.String({
    description: '任务唯一标识符',
  }),
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
});

const ResultNotFoundSchema = t.Object({
  success: t.Literal(false),
  error: t.Literal('TASK_NOT_FOUND'),
});

const GenericErrorSchema = t.Object({
  success: t.Boolean(),
  error: t.String(),
});

export const resultsRoute = (app: Elysia, db: Db): Elysia => {
  app.post(
    '/api/v1/results',
    async ({ body, request, set }) => {
      const traceId = extractTraceId(request.headers) ?? generateTraceId();
      const callDepth = validateCallDepthFromHeaders(request.headers);
      if (!callDepth.ok) {
        const invalidDepthAudit: AuditEventInput = {
          ts: Date.now(),
          level: 'WARN',
          node_id: 'core',
          source: 'results',
          trace_id: traceId,
          content: 'Rejected result request due to invalid call_depth',
          meta: {
            reason: callDepth.reason,
            raw_call_depth: callDepth.raw ?? '',
          },
        };

        try {
          await recordAuditEvent(db, invalidDepthAudit, { routeTag: 'results' });
        } catch (auditError) {
          console.error('[Audit] failed to log invalid call_depth rejection', auditError);
        }

        return respondWithCode(set, 'INVALID_CALL_DEPTH');
      }

      const payload: TaskResultPayload = {
        status: body.status as TaskResultStatus,
        output: body.output,
        error: body.error,
      };

      let taskResult;
      try {
        if (isAuditPipelineReady()) {
          const txResult = await runInTransaction(db, async (session) => {
            const updatedTask = await submitResult(db, body.task_id, payload, session);
            if (!updatedTask) {
              return null;
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
            return updatedTask;
          });
          taskResult = txResult;
        } else {
          taskResult = await submitResult(db, body.task_id, payload);
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

      if (!taskResult) {
        return respondWithCode(set, 'TASK_NOT_FOUND');
      }

      const meta: Record<string, unknown> = {
        task_id: body.task_id,
        status: payload.status,
        result_uri: taskResult.result_uri,
        call_depth: callDepth.depth,
      };

      if (taskResult.result_error) {
        meta.result_error = taskResult.result_error;
      }

      if (!isAuditPipelineReady()) {
        const auditEvent: AuditEventInput = {
          ts: Date.now(),
          level: 'INFO',
          node_id: taskResult.target_node_id || 'core',
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

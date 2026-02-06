import { Elysia, t } from 'elysia';
import { Db } from 'mongodb';

import { submitResult, type TaskResultPayload, type TaskResultStatus } from '../services/result-handler';
import { logAuditEvent, type AuditEventInput } from '../services/audit';
import { extractTraceId, generateTraceId } from '../utils/trace-context';

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
  error: t.Literal('Task not found'),
});

const GenericErrorSchema = t.Object({
  success: t.Boolean(),
  error: t.String(),
});

export const resultsRoute = (app: Elysia): Elysia => {
  app.post(
    '/api/v1/results',
    async ({ body, request, set }) => {
      const db = (global as { db?: Db }).db;

      if (!db) {
        set.status = 500;
        return {
          success: false,
          error: 'DATABASE_NOT_CONNECTED',
        };
      }

      const traceId = extractTraceId(request.headers) ?? generateTraceId();
      const payload: TaskResultPayload = {
        status: body.status as TaskResultStatus,
        output: body.output,
        error: body.error,
      };

      let taskResult;
      try {
        taskResult = await submitResult(db, body.task_id, payload);
      } catch (error) {
        console.error('[Results] failed to persist task result', error);
        set.status = 500;
        return {
          success: false,
          error: 'RESULT_SUBMISSION_FAILED',
        };
      }

      if (!taskResult) {
        set.status = 404;
        return {
          success: false,
          error: 'Task not found',
        };
      }

      const meta: Record<string, unknown> = {
        task_id: body.task_id,
        status: payload.status,
        result_uri: taskResult.result_uri,
      };

      if (taskResult.result_error) {
        meta.result_error = taskResult.result_error;
      }

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
        await logAuditEvent(db, auditEvent);
      } catch (auditError) {
        console.error('[Audit] failed to log task result', auditError);
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
        404: ResultNotFoundSchema,
        500: GenericErrorSchema,
      },
    },
  );

  return app;
};

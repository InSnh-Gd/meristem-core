import { Elysia, t } from 'elysia';
import type { Db } from 'mongodb';

import { extractTraceId, generateTraceId } from '../utils/trace-context';
import { generateBootstrapToken, validateBootstrapToken, createFirstUser } from '../services/bootstrap';
import {
  logAuditEvent,
  type AuditEventInput,
  type AuditLog,
} from '../services/audit';

export const BootstrapRequestBodySchema = t.Object({
  bootstrap_token: t.String({
    description: 'Bootstrap token in the ST-XXXX-XXXX format',
    minLength: 12,
    maxLength: 12,
  }),
  username: t.String({
    description: 'Admin username to create during bootstrap',
    minLength: 3,
    maxLength: 128,
  }),
  password: t.String({
    description: 'Strong password for the initial admin account',
    minLength: 8,
    maxLength: 128,
  }),
});

export const BootstrapSuccessResponseSchema = t.Object({
  success: t.Literal(true),
  user_id: t.String(),
  created_at: t.String(),
});

export const BootstrapErrorResponseSchema = t.Object({
  success: t.Literal(false),
  error: t.String(),
});

type AuditLogger = (db: Db, event: AuditEventInput) => Promise<AuditLog>;

export const bootstrapRoute = (
  app: Elysia,
  db: Db,
  auditLogger: AuditLogger = logAuditEvent,
): Elysia => {
  app.post(
    '/api/v1/auth/bootstrap',
    async ({ body, request, set }) => {
      const { bootstrap_token, username, password } = body;

      if (!validateBootstrapToken(bootstrap_token)) {
        set.status = 400;
        return {
          success: false,
          error: 'Invalid bootstrap token',
        };
      }

      let user: Awaited<ReturnType<typeof createFirstUser>>;
      try {
        user = await createFirstUser(db, username, password);
      } catch (error) {
        if (error instanceof Error && error.message === 'bootstrap already completed') {
          set.status = 409;
          return {
            success: false,
            error: 'Bootstrap already completed',
          };
        }

        console.error('[Bootstrap] failed to create first user', error);
        set.status = 500;
        return {
          success: false,
          error: 'Unable to complete bootstrap',
        };
      }

      const traceId = extractTraceId(request.headers) ?? generateTraceId();
      const auditEvent: AuditEventInput = {
        ts: Date.now(),
        level: 'INFO',
        node_id: 'core',
        source: 'bootstrap',
        trace_id: traceId,
        content: 'Bootstrap completed',
        meta: {
          username,
          user_id: user.user_id,
          bootstrap_token,
          example_token: generateBootstrapToken(),
        },
      };

      try {
        await auditLogger(db, auditEvent);
      } catch (auditError) {
        console.error('[Audit] failed to log bootstrap completion', auditError);
      }

      set.status = 201;
      return {
        success: true,
        user_id: user.user_id,
        created_at: user.created_at.toISOString(),
      };
    },
    {
      body: BootstrapRequestBodySchema,
      response: {
        201: BootstrapSuccessResponseSchema,
        400: BootstrapErrorResponseSchema,
        409: BootstrapErrorResponseSchema,
        500: BootstrapErrorResponseSchema,
      },
    },
  );

  return app;
};

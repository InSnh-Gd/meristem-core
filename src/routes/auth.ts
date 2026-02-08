import { Elysia, t } from 'elysia';
import { Db } from 'mongodb';
import { extractTraceId, generateTraceId } from '../utils/trace-context';
import { authenticateUser, generateJWT } from '../services/auth';
import { logAuditEvent, type AuditEventInput, type AuditLog } from '../services/audit';

const TOKEN_EXPIRATION_SECONDS = 24 * 60 * 60;

export const LoginRequestBodySchema = t.Object({
  username: t.String({
    description: 'Login username',
    minLength: 1,
  }),
  password: t.String({
    description: 'Login password',
    minLength: 1,
  }),
});

export const LoginSuccessResponseSchema = t.Object({
  success: t.Literal(true),
  access_token: t.String(),
  token_type: t.Literal('Bearer'),
  expires_in: t.Number(),
});

export const LoginErrorResponseSchema = t.Object({
  success: t.Literal(false),
  error: t.String(),
});

type AuditLogger = (db: Db, event: AuditEventInput) => Promise<AuditLog>;

export const authRoute = (
  app: Elysia,
  db: Db,
  auditLogger: AuditLogger = logAuditEvent,
): Elysia => {
  app.post(
    '/api/v1/auth/login',
    async ({ body, set, request }) => {
      const { username, password } = body;

      const traceId = extractTraceId(request.headers) ?? generateTraceId();
      const now = Date.now();
      const user = await authenticateUser(db, username, password);

      if (!user) {
        set.status = 401;

        const auditEvent: AuditEventInput = {
          ts: now,
          level: 'WARN',
          node_id: username,
          source: 'auth',
          trace_id: traceId,
          content: 'Invalid login attempt',
          meta: {
            username,
            outcome: 'invalid_credentials',
          },
        };

        try {
          await auditLogger(db, auditEvent);
        } catch (auditError) {
          console.error('[Audit] failed to log auth failure', auditError);
        }

        return {
          success: false,
          error: 'Invalid credentials',
        };
      }

      const access_token = await generateJWT(user);

      const auditEvent: AuditEventInput = {
        ts: now,
        level: 'INFO',
        node_id: user.user_id,
        source: 'auth',
        trace_id: traceId,
        content: 'User authenticated',
        meta: {
          username: user.username,
          outcome: 'success',
        },
      };

      try {
        await auditLogger(db, auditEvent);
      } catch (auditError) {
        console.error('[Audit] failed to log auth success', auditError);
      }

      return {
        success: true,
        access_token,
        token_type: 'Bearer',
        expires_in: TOKEN_EXPIRATION_SECONDS,
      };
    },
    {
      body: LoginRequestBodySchema,
      response: {
        200: LoginSuccessResponseSchema,
        401: LoginErrorResponseSchema,
      },
    },
  );

  return app;
};

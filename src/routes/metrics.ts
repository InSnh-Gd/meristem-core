import { Elysia, t } from 'elysia';
import { renderDbMetricsPrometheus } from '../db/observability';
import { requireAuth } from '../middleware/auth';
import { ensureSuperadminAccess } from './route-auth';

const MetricsErrorSchema = t.Object({
  success: t.Literal(false),
  error: t.String(),
});

export const metricsRoute = (app: Elysia): Elysia => {
  app.get(
    '/metrics',
    ({ set, store }) => {
      const denied = ensureSuperadminAccess(store, set);
      if (denied) {
        return denied;
      }

      set.headers['content-type'] = 'text/plain; version=0.0.4; charset=utf-8';
      return renderDbMetricsPrometheus();
    },
    {
      response: {
        200: t.String(),
        401: MetricsErrorSchema,
        403: MetricsErrorSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  return app;
};

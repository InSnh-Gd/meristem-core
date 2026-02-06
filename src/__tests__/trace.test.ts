import { Elysia } from 'elysia';
import { traceMiddleware, type TraceMiddlewareContext } from '../middleware/trace';
import type { Context } from 'elysia';

const TRACE_PATH = 'http://localhost/trace';
const TRACE_ID_PATTERN = /^trace-\d+-[a-z0-9]{8}$/;

type TraceContextEnabledContext = TraceMiddlewareContext & Context;

const readTraceContext = (ctx: TraceContextEnabledContext) => ctx.traceContext;
type TraceRouteHandler = (ctx: TraceContextEnabledContext) => unknown;

const buildApp = (handler?: TraceRouteHandler) => {
  const app = new Elysia();
  app.use(traceMiddleware());
  app.get('/trace', (ctx: TraceContextEnabledContext) =>
    handler ? handler(ctx) : readTraceContext(ctx),
  );
  return app;
};

test('trace middleware exposes header trace id via ctx.traceContext', async () => {
  const traceId = 'header-abc-123';
  const app = buildApp();

  const response = await app.handle(
    new Request(TRACE_PATH, {
      headers: {
        'X-Trace-Id': traceId,
      },
    }),
  );

  expect(response.status).toBe(200);
  const body = await response.json();

  expect(body).toEqual({
    traceId,
    nodeId: 'core',
    source: 'http',
  });
});

test('trace middleware generates trace id when header missing', async () => {
  const app = buildApp();

  const response = await app.handle(new Request(TRACE_PATH));
  expect(response.status).toBe(200);

  const body = await response.json();
  expect(typeof body.traceId).toBe('string');
  expect(body.traceId).toMatch(TRACE_ID_PATTERN);
  expect(body.nodeId).toBe('core');
  expect(body.source).toBe('http');
});

test('trace context stays frozen and unique per request', async () => {
  const app = buildApp((ctx) => ({
    traceId: ctx.traceContext.traceId,
    frozen: Object.isFrozen(ctx.traceContext),
  }));

  const first = await app.handle(new Request(TRACE_PATH));
  const second = await app.handle(new Request(TRACE_PATH));

  const firstBody = await first.json();
  const secondBody = await second.json();

  expect(firstBody.traceId).not.toBe(secondBody.traceId);
  expect(firstBody.frozen).toBe(true);
  expect(secondBody.frozen).toBe(true);
});

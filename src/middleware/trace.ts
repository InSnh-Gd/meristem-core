import { Elysia } from 'elysia';
import {
  createTraceContext,
  extractTraceId,
  generateTraceId,
  type TraceContext,
} from '../utils/trace-context';

const NODE_ID = 'core';
const SOURCE = 'http';

export type TraceMiddlewareContext = {
  traceContext: TraceContext;
};

export const traceMiddleware = () =>
  (app: Elysia) =>
    app.derive(({ request }) => {
      const incomingTraceId = extractTraceId(request.headers);
      const traceId = incomingTraceId ?? generateTraceId();

      const traceContext = createTraceContext({
        traceId,
        nodeId: NODE_ID,
        source: SOURCE,
      });

      return { traceContext };
    });

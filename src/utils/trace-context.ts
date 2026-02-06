const TRACE_HEADER = 'x-trace-id';

type TraceContextFields = {
  traceId: string;
  nodeId: string;
  source: string;
  taskId?: string;
};

type TraceContextInput = {
  traceId?: string;
  nodeId: string;
  source: string;
  taskId?: string;
};

export type TraceContext = Readonly<TraceContextFields>;

const freezeTraceContext = (context: TraceContextFields): TraceContext =>
  Object.freeze({ ...context });

export function generateTraceId(): string {
  const timestamp = Date.now();
  const randomSuffix = Math.random().toString(36).substring(2, 10);
  return `trace-${timestamp}-${randomSuffix}`;
}

export function createTraceContext(props: TraceContextInput): TraceContext {
  const base: TraceContextFields = {
    traceId: props.traceId ?? generateTraceId(),
    nodeId: props.nodeId,
    source: props.source,
    taskId: props.taskId,
  };
  return freezeTraceContext(base);
}

export function extractTraceId(headers: Headers): string | undefined {
  if (!headers) {
    return undefined;
  }
  return headers.get(TRACE_HEADER) ?? undefined;
}

export function withTaskId(ctx: TraceContext, taskId: string): TraceContext {
  return freezeTraceContext({ ...ctx, taskId });
}

export function withSource(ctx: TraceContext, source: string): TraceContext {
  return freezeTraceContext({ ...ctx, source });
}

export function withNodeId(ctx: TraceContext, nodeId: string): TraceContext {
  return freezeTraceContext({ ...ctx, nodeId });
}

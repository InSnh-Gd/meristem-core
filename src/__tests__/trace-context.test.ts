import {
  createTraceContext,
  extractTraceId,
  generateTraceId,
  withNodeId,
  withSource,
  withTaskId,
} from '../utils/trace-context';

const TRACE_ID_PATTERN = /^trace-\d+-[a-z0-9]+$/;

test('generateTraceId produces unique ids that follow the expected pattern', () => {
  const seen = new Set<string>();

  for (let i = 0; i < 100; i += 1) {
    const id = generateTraceId();
    expect(id).toMatch(TRACE_ID_PATTERN);
    seen.add(id);
  }

  expect(seen.size).toBe(100);
});

test('extractTraceId reads X-Trace-Id header case-insensitively', () => {
  const uppercase = new Headers();
  uppercase.set('X-Trace-Id', 'trace-uppercase');
  expect(extractTraceId(uppercase)).toBe('trace-uppercase');

  const lowercase = new Headers();
  lowercase.set('x-trace-id', 'trace-lowercase');
  expect(extractTraceId(lowercase)).toBe('trace-lowercase');

  const empty = new Headers();
  expect(extractTraceId(empty)).toBeUndefined();
});

test('createTraceContext respects supplied values and freezes the result', () => {
  const context = createTraceContext({
    traceId: 'trace-custom',
    nodeId: 'node-a',
    source: 'service-a',
  });

  expect(context.traceId).toBe('trace-custom');
  expect(context.nodeId).toBe('node-a');
  expect(context.source).toBe('service-a');
  expect(Object.isFrozen(context)).toBe(true);

  expect(() => {
    (context as unknown as { traceId: string }).traceId = 'new-value';
  }).toThrow();
});

test('derivation helpers return new, frozen contexts and leave originals untouched', () => {
  const base = createTraceContext({ nodeId: 'node-base', source: 'service-base' });

  const taskContext = withTaskId(base, 'task-123');
  expect(taskContext.taskId).toBe('task-123');
  expect(base.taskId).toBeUndefined();
  expect(taskContext).not.toBe(base);
  expect(Object.isFrozen(taskContext)).toBe(true);

  const sourceContext = withSource(taskContext, 'service-updated');
  expect(sourceContext.source).toBe('service-updated');
  expect(taskContext.source).toBe('service-base');
  expect(sourceContext).not.toBe(taskContext);
  expect(Object.isFrozen(sourceContext)).toBe(true);

  const nodeContext = withNodeId(sourceContext, 'node-updated');
  expect(nodeContext.nodeId).toBe('node-updated');
  expect(sourceContext.nodeId).toBe('node-base');
  expect(nodeContext).not.toBe(sourceContext);
  expect(Object.isFrozen(nodeContext)).toBe(true);
});

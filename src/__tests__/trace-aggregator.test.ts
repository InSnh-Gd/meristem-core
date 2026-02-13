import { describe, expect, test } from 'bun:test';
import type { LogEnvelope } from '@insnh-gd/meristem-shared';
import type { JsMsg, NatsConnection } from 'nats';
import { TraceAggregator } from '../services/trace-aggregator';

class FakeSubscription implements AsyncIterable<JsMsg> {
  private readonly queue: JsMsg[] = [];
  private readonly waiters: Array<(msg: JsMsg) => void> = [];
  private unsubscribed = false;

  [Symbol.asyncIterator](): AsyncIterator<JsMsg> {
    return {
      next: async (): Promise<IteratorResult<JsMsg>> => {
        if (this.unsubscribed) {
          return { done: true, value: undefined };
        }

        const next = this.queue.shift();
        if (next) {
          return { done: false, value: next };
        }

        return await new Promise<IteratorResult<JsMsg>>((resolve) => {
          this.waiters.push((msg) => resolve({ done: false, value: msg }));
        });
      },
    };
  }

  publish(msg: JsMsg): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(msg);
      return;
    }

    this.queue.push(msg);
  }

  unsubscribe(): void {
    this.unsubscribed = true;
  }

  isUnsubscribed(): boolean {
    return this.unsubscribed;
  }
}

const createLog = (traceId: string, ts = Date.now()): LogEnvelope => ({
  ts,
  level: 'INFO',
  node_id: 'core',
  source: 'test',
  trace_id: traceId,
  content: `log-${traceId}`,
  meta: {},
});

describe('TraceAggregator', () => {
  test('aggregates messages from trace subject', async () => {
    const subscription = new FakeSubscription();
    const connection = {
      subscribe: () => subscription,
    } as unknown as NatsConnection;

    const aggregator = new TraceAggregator(connection);
    const log = createLog('trace-a');

    subscription.publish({
      subject: 'meristem.v1.logs.trace.trace-a',
      data: new TextEncoder().encode(JSON.stringify(log)),
    } as unknown as JsMsg);

    await Bun.sleep(5);
    const traceLogs = await aggregator.queryByTraceId('trace-a');

    expect(traceLogs).toHaveLength(1);
    expect(traceLogs[0]?.trace_id).toBe('trace-a');

    aggregator.stop();
  });

  test('evicts oldest trace when maxTraces reached', async () => {
    const subscription = new FakeSubscription();
    const connection = {
      subscribe: () => subscription,
    } as unknown as NatsConnection;

    const aggregator = new TraceAggregator(connection, { maxTraces: 1 });
    aggregator.aggregate('trace-1', [createLog('trace-1', 1)]);
    aggregator.aggregate('trace-2', [createLog('trace-2', 2)]);

    expect(await aggregator.queryByTraceId('trace-1')).toHaveLength(0);
    expect(await aggregator.queryByTraceId('trace-2')).toHaveLength(1);

    aggregator.stop();
    expect(subscription.isUnsubscribed()).toBe(true);
  });
});

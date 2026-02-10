import { expect, test } from 'bun:test';
import type { Subscription } from 'nats';
import { toManagedSubscription } from '../nats/connection';

test('toManagedSubscription unsubscribes on dispose', (): void => {
  let unsubscribeCalls = 0;
  const subscription = {
    unsubscribe: (): void => {
      unsubscribeCalls += 1;
    },
  } as unknown as Subscription;

  const managed = toManagedSubscription(subscription);
  managed[Symbol.dispose]();

  expect(unsubscribeCalls).toBe(1);
});


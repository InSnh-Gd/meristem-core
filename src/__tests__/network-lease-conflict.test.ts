import { expect, test } from 'bun:test';
import type { Db } from 'mongodb';
import { checkNodeOffline } from '../services/heartbeat';
import { createTraceContext } from '../utils/trace-context';

test('checkNodeOffline applies soft reclamation and increments generation', async (): Promise<void> => {
  const updateCalls: Array<{ filter: Record<string, unknown>; update: Record<string, unknown> }> = [];

  const collection = {
    updateMany: async (
      filter: Record<string, unknown>,
      update: Record<string, unknown>,
    ): Promise<{ modifiedCount: number }> => {
      updateCalls.push({ filter, update });
      return { modifiedCount: 1 };
    },
    find: (): { toArray: () => Promise<Array<{ node_id: string }>> } => ({
      toArray: async () => [{ node_id: 'node-offline' }],
    }),
  };

  const mockDb = {
    collection: () => collection,
  } as unknown as Db;

  const trace = createTraceContext({
    traceId: 'test-trace',
    nodeId: 'core',
    source: 'test',
  });

  const offline = await checkNodeOffline(mockDb, trace, 1_000);

  expect(offline).toEqual(['node-offline']);
  expect(updateCalls).toHaveLength(2);

  const reclaimUpdate = updateCalls[1]?.update;
  expect(reclaimUpdate).toBeDefined();
  expect((reclaimUpdate?.$set as Record<string, unknown>)['status.connection_status']).toBe(
    'expired_credentials',
  );
  expect((reclaimUpdate?.$set as Record<string, unknown>)['network.ip_shadow_lease.reclaim_status']).toBe(
    'RECLAIMED',
  );
  expect((reclaimUpdate?.$inc as Record<string, unknown>)['network.ip_shadow_lease.reclaim_generation']).toBe(
    1,
  );
});

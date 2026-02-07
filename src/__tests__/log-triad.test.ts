import { expect, test } from 'bun:test';
import {
  applyBroadStrokesFilter,
  isPulseSnapshotPayload,
  toSnapshotMeta,
  type PulseSnapshotPayload,
} from '../services/log-triad';

test('isPulseSnapshotPayload validates required pulse fields', (): void => {
  const validPayload: PulseSnapshotPayload = {
    node_id: 'node-1',
    ts: Date.now(),
    core: {
      cpu_load: 0.32,
      ram_usage: 0.61,
      net_io: {
        in: 1200,
        out: 900,
      },
    },
    plugins: {
      gpu: { temp: 70 },
    },
  };

  expect(isPulseSnapshotPayload(validPayload)).toBe(true);
  expect(isPulseSnapshotPayload({ node_id: 'node-1' })).toBe(false);
  expect(isPulseSnapshotPayload(null)).toBe(false);
});

test('toSnapshotMeta converts pulse payload into snapshot meta', (): void => {
  const payload: PulseSnapshotPayload = {
    node_id: 'node-2',
    ts: Date.now(),
    core: {
      cpu_load: 0.1119,
      ram_usage: 0.8899,
      net_io: {
        in: 1000,
        out: 2000,
      },
    },
    plugins: {
      cache: { hit_rate: 0.9 },
      gpu: { usage: 0.5 },
    },
  };

  const meta = toSnapshotMeta(payload);

  expect(meta.node_id).toBe('node-2');
  expect(meta.cpu_load).toBe(0.112);
  expect(meta.ram_usage).toBe(0.89);
  expect(meta.net_in).toBe(1000);
  expect(meta.net_out).toBe(2000);
  expect(meta.plugin_count).toBe(2);
});

test('applyBroadStrokesFilter clamps out-of-range metrics', (): void => {
  const filtered = applyBroadStrokesFilter({
    node_id: 'node-3',
    ts: Date.now(),
    cpu_load: 1.5,
    ram_usage: -0.3,
    net_in: 10,
    net_out: 20,
    plugin_count: 0,
  });

  expect(filtered.cpu_load).toBe(1);
  expect(filtered.ram_usage).toBe(0);
});

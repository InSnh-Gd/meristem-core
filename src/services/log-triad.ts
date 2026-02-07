export type PulseSnapshotPayload = {
  node_id: string;
  ts: number;
  core: {
    cpu_load: number;
    ram_usage: number;
    net_io?: {
      in: number;
      out: number;
    };
  };
  plugins?: Record<string, unknown>;
};

export type SnapshotMeta = {
  node_id: string;
  ts: number;
  cpu_load: number;
  ram_usage: number;
  net_in: number;
  net_out: number;
  plugin_count: number;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === 'number' && Number.isFinite(value);

const round = (value: number): number => Math.round(value * 1000) / 1000;

const clamp01 = (value: number): number => {
  if (value < 0) {
    return 0;
  }
  if (value > 1) {
    return 1;
  }
  return value;
};

export const isPulseSnapshotPayload = (value: unknown): value is PulseSnapshotPayload => {
  if (!isRecord(value)) {
    return false;
  }

  if (typeof value.node_id !== 'string' || !isFiniteNumber(value.ts) || !isRecord(value.core)) {
    return false;
  }

  const core = value.core;
  if (!isFiniteNumber(core.cpu_load) || !isFiniteNumber(core.ram_usage)) {
    return false;
  }

  if (core.net_io !== undefined) {
    if (!isRecord(core.net_io)) {
      return false;
    }
    if (!isFiniteNumber(core.net_io.in) || !isFiniteNumber(core.net_io.out)) {
      return false;
    }
  }

  if (value.plugins !== undefined && !isRecord(value.plugins)) {
    return false;
  }

  return true;
};

export const toSnapshotMeta = (payload: PulseSnapshotPayload): SnapshotMeta => ({
  node_id: payload.node_id,
  ts: payload.ts,
  cpu_load: round(payload.core.cpu_load),
  ram_usage: round(payload.core.ram_usage),
  net_in: payload.core.net_io?.in ?? 0,
  net_out: payload.core.net_io?.out ?? 0,
  plugin_count: payload.plugins ? Object.keys(payload.plugins).length : 0,
});

export const applyBroadStrokesFilter = (meta: SnapshotMeta): SnapshotMeta => ({
  ...meta,
  cpu_load: clamp01(meta.cpu_load),
  ram_usage: clamp01(meta.ram_usage),
});

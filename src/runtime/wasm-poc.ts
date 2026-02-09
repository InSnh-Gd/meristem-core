import { evaluateWasmGate, type WasmBenefit, type WasmGateResult, type WasmTiming } from './wasm-gate';

export type WasmHotspotName = 'audit-hash-batch' | 'nats-payload-codec';

export type WasmPocHotspotResult = {
  name: WasmHotspotName;
  iterations: number;
  timing: WasmTiming & {
    totalMs: number;
  };
  gate: WasmGateResult;
};

export type WasmPocBenchmarkResult = {
  hotspots: readonly WasmPocHotspotResult[];
};

export type RunWasmPocOptions = {
  iterations?: number;
  benefitByHotspot?: Partial<Record<WasmHotspotName, WasmBenefit>>;
};

const DEFAULT_ITERATIONS = 2_000;

const now = (): number => performance.now();

const toBenefit = (value: WasmBenefit | undefined): WasmBenefit =>
  value ?? {
    throughputDeltaRatio: 0,
    p95DeltaRatio: 0,
    cpuTimeDeltaRatio: 0,
  };

const runAuditHashBatch = (iterations: number): WasmTiming => {
  const source = new Uint8Array(1024);
  source.fill(7);

  let marshalMs = 0;
  let computeMs = 0;
  let unmarshalMs = 0;

  for (let index = 0; index < iterations; index += 1) {
    const marshalStart = now();
    const input = source.subarray(0);
    marshalMs += now() - marshalStart;

    const computeStart = now();
    let hash = 2166136261;
    for (let offset = 0; offset < input.byteLength; offset += 1) {
      hash ^= input[offset] ?? 0;
      hash = (hash * 16777619) >>> 0;
    }
    computeMs += now() - computeStart;

    const unmarshalStart = now();
    const output = new Uint8Array(4);
    output[0] = hash & 0xff;
    output[1] = (hash >>> 8) & 0xff;
    output[2] = (hash >>> 16) & 0xff;
    output[3] = (hash >>> 24) & 0xff;
    if (output[0] === 255 && output[1] === 255) {
      throw new Error('unexpected hash output');
    }
    unmarshalMs += now() - unmarshalStart;
  }

  return {
    marshalMs,
    computeMs,
    unmarshalMs,
  };
};

const runNatsCodec = (iterations: number): WasmTiming => {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const payload = JSON.stringify({
    node_id: 'poc-node',
    ts: Date.now(),
    metrics: {
      cpu: 0.42,
      mem: 0.61,
    },
  });

  let marshalMs = 0;
  let computeMs = 0;
  let unmarshalMs = 0;

  for (let index = 0; index < iterations; index += 1) {
    const marshalStart = now();
    const bytes = encoder.encode(payload);
    marshalMs += now() - marshalStart;

    const computeStart = now();
    let checksum = 0;
    for (let offset = 0; offset < bytes.byteLength; offset += 1) {
      checksum = (checksum + (bytes[offset] ?? 0)) % 1_000_003;
    }
    computeMs += now() - computeStart;

    const unmarshalStart = now();
    const decoded = decoder.decode(bytes);
    if (decoded.length === 0 || checksum < 0) {
      throw new Error('invalid codec output');
    }
    unmarshalMs += now() - unmarshalStart;
  }

  return {
    marshalMs,
    computeMs,
    unmarshalMs,
  };
};

const toHotspotResult = (
  name: WasmHotspotName,
  iterations: number,
  timing: WasmTiming,
  benefit: WasmBenefit,
): WasmPocHotspotResult => {
  const gate = evaluateWasmGate(timing, benefit);
  return {
    name,
    iterations,
    timing: {
      ...timing,
      totalMs: gate.totalMs,
    },
    gate,
  };
};

export const runWasmPocBenchmarks = (options: RunWasmPocOptions = {}): WasmPocBenchmarkResult => {
  const iterations = options.iterations ?? DEFAULT_ITERATIONS;
  const benefitByHotspot = options.benefitByHotspot ?? {};

  const auditTiming = runAuditHashBatch(iterations);
  const codecTiming = runNatsCodec(iterations);

  return {
    hotspots: [
      toHotspotResult('audit-hash-batch', iterations, auditTiming, toBenefit(benefitByHotspot['audit-hash-batch'])),
      toHotspotResult('nats-payload-codec', iterations, codecTiming, toBenefit(benefitByHotspot['nats-payload-codec'])),
    ],
  };
};

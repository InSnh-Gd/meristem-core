import { runWasmPocBenchmarks, type WasmPocBenchmarkResult } from '../runtime/wasm-poc';

type BenchmarkSample = {
  name: string;
  iterations: number;
  durationMs: number;
  opsPerSecond: number;
};

type BenchmarkReport = {
  generatedAt: string;
  runtime: {
    bunVersion: string;
    platform: string;
    arch: string;
  };
  samples: readonly BenchmarkSample[];
  wasmPoc: WasmPocBenchmarkResult;
};

const runBenchmark = (
  name: string,
  iterations: number,
  runner: (index: number) => void,
): BenchmarkSample => {
  const start = performance.now();
  for (let index = 0; index < iterations; index += 1) {
    runner(index);
  }
  const durationMs = performance.now() - start;
  const opsPerSecond = iterations / (durationMs / 1000);
  return {
    name,
    iterations,
    durationMs,
    opsPerSecond,
  };
};

const runBaseline = (): BenchmarkReport => {
  const payload = {
    node_id: 'bench-node',
    ts: Date.now(),
    metrics: {
      cpu: 0.42,
      mem: 0.65,
      disk: 0.08,
    },
    tags: ['bench', 'runtime'],
  };
  const byteSource = new Uint8Array(4096);
  const wasmPoc = runWasmPocBenchmarks({
    iterations: 600,
  });

  const samples: BenchmarkSample[] = [
    runBenchmark('json-stringify-parse', 20_000, () => {
      const encoded = JSON.stringify(payload);
      const decoded = JSON.parse(encoded) as Record<string, unknown>;
      if (!decoded.node_id) {
        throw new Error('invalid decode');
      }
    }),
    runBenchmark('uint8array-copy', 50_000, () => {
      const copied = byteSource.slice();
      if (copied.byteLength !== byteSource.byteLength) {
        throw new Error('invalid copy');
      }
    }),
    runBenchmark('text-encode-decode', 50_000, () => {
      const encoded = new TextEncoder().encode('meristem-benchmark');
      const decoded = new TextDecoder().decode(encoded);
      if (decoded.length === 0) {
        throw new Error('invalid text decode');
      }
    }),
  ];

  return {
    generatedAt: new Date().toISOString(),
    runtime: {
      bunVersion: Bun.version,
      platform: process.platform,
      arch: process.arch,
    },
    samples,
    wasmPoc,
  };
};

const report = runBaseline();
console.log(JSON.stringify(report, null, 2));

import { expect, test } from 'bun:test';
import { runWasmPocBenchmarks } from '../runtime/wasm-poc';

test('runWasmPocBenchmarks returns dual hotspot metrics and gate decision', (): void => {
  const result = runWasmPocBenchmarks({
    iterations: 200,
  });

  expect(result.hotspots).toHaveLength(2);
  expect(result.hotspots[0]?.name).toBe('audit-hash-batch');
  expect(result.hotspots[1]?.name).toBe('nats-payload-codec');

  for (const hotspot of result.hotspots) {
    expect(hotspot.timing.totalMs).toBeGreaterThanOrEqual(0);
    expect(hotspot.gate.serializationRatio).toBeGreaterThanOrEqual(0);
    expect(hotspot.gate.serializationRatio).toBeLessThanOrEqual(1);
  }
});

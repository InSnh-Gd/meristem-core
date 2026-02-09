import { expect, test } from 'bun:test';
import { evaluateWasmGate } from '../runtime/wasm-gate';

test('evaluateWasmGate disables wasm when serialization cost is dominant and no benefit', (): void => {
  const result = evaluateWasmGate(
    {
      marshalMs: 9,
      computeMs: 4,
      unmarshalMs: 5,
    },
    {
      throughputDeltaRatio: 0,
      p95DeltaRatio: 0,
      cpuTimeDeltaRatio: 0,
    },
  );

  expect(result.serializationRatio).toBeGreaterThan(0.4);
  expect(result.shouldDisable).toBe(true);
});

test('evaluateWasmGate keeps wasm when endpoint metrics improve', (): void => {
  const result = evaluateWasmGate(
    {
      marshalMs: 6,
      computeMs: 4,
      unmarshalMs: 4,
    },
    {
      throughputDeltaRatio: 0.25,
      p95DeltaRatio: 0,
      cpuTimeDeltaRatio: 0,
    },
  );

  expect(result.serializationRatio).toBeGreaterThan(0.4);
  expect(result.shouldDisable).toBe(false);
});


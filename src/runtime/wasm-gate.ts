export type WasmTiming = {
  marshalMs: number;
  computeMs: number;
  unmarshalMs: number;
};

export type WasmBenefit = {
  throughputDeltaRatio: number;
  p95DeltaRatio: number;
  cpuTimeDeltaRatio: number;
};

export type WasmGateResult = {
  totalMs: number;
  serializationRatio: number;
  noEndpointBenefit: boolean;
  shouldDisable: boolean;
};

const clampToZero = (value: number): number => (value > 0 ? value : 0);

const hasEndpointBenefit = (benefit: WasmBenefit): boolean => {
  const throughputImproved = benefit.throughputDeltaRatio > 0;
  const p95Improved = benefit.p95DeltaRatio < 0;
  const cpuImproved = benefit.cpuTimeDeltaRatio < 0;
  return throughputImproved || p95Improved || cpuImproved;
};

export const evaluateWasmGate = (
  timing: WasmTiming,
  benefit: WasmBenefit,
): WasmGateResult => {
  const marshalMs = clampToZero(timing.marshalMs);
  const computeMs = clampToZero(timing.computeMs);
  const unmarshalMs = clampToZero(timing.unmarshalMs);
  const totalMs = marshalMs + computeMs + unmarshalMs;
  const serializationMs = marshalMs + unmarshalMs;
  const serializationRatio = totalMs === 0 ? 0 : serializationMs / totalMs;
  const noEndpointBenefit = !hasEndpointBenefit(benefit);
  const shouldDisable = serializationRatio > 0.4 && noEndpointBenefit;

  return {
    totalMs,
    serializationRatio,
    noEndpointBenefit,
    shouldDisable,
  };
};


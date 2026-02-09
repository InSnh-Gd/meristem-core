import { expect, test } from 'bun:test';
import {
  getFeatureFlagDefaults,
  isFeatureEnabled,
  resolveFeatureFlags,
} from '../config/feature-flags';

test('resolveFeatureFlags uses defaults when env is empty', (): void => {
  const flags = resolveFeatureFlags({});
  expect(flags).toEqual(getFeatureFlagDefaults());
});

test('resolveFeatureFlags parses truthy and falsy values', (): void => {
  const flags = resolveFeatureFlags({
    ENABLE_EDEN_WS: 'true',
    ENABLE_EFFECT_RPC_POC: '1',
    ENABLE_FASTPATH_HEARTBEAT: 'false',
    ENABLE_WASM_POC: '0',
    ENABLE_SAB_EXPERIMENT: 'yes',
  });

  expect(flags.ENABLE_EDEN_WS).toBe(true);
  expect(flags.ENABLE_EFFECT_RPC_POC).toBe(true);
  expect(flags.ENABLE_FASTPATH_HEARTBEAT).toBe(false);
  expect(flags.ENABLE_WASM_POC).toBe(false);
  expect(flags.ENABLE_SAB_EXPERIMENT).toBe(true);
});

test('isFeatureEnabled falls back to defaults on invalid value', (): void => {
  const enabled = isFeatureEnabled('ENABLE_FASTPATH_HEARTBEAT', {
    ENABLE_FASTPATH_HEARTBEAT: 'invalid-bool',
  });
  expect(enabled).toBe(false);
});


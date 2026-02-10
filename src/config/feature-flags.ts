type EnvMap = Readonly<Record<string, string | undefined>>;

export type FeatureFlagName =
  | 'ENABLE_EDEN_WS'
  | 'ENABLE_EFFECT_RPC_POC'
  | 'ENABLE_FASTPATH_HEARTBEAT'
  | 'ENABLE_WASM_POC'
  | 'ENABLE_SAB_EXPERIMENT';

export type FeatureFlags = Readonly<Record<FeatureFlagName, boolean>>;

const TRUE_VALUES = new Set(['1', 'true', 'yes', 'on', 'enabled']);
const FALSE_VALUES = new Set(['0', 'false', 'no', 'off', 'disabled']);

const FEATURE_FLAG_DEFAULTS: FeatureFlags = Object.freeze({
  ENABLE_EDEN_WS: false,
  ENABLE_EFFECT_RPC_POC: false,
  ENABLE_FASTPATH_HEARTBEAT: false,
  ENABLE_WASM_POC: false,
  ENABLE_SAB_EXPERIMENT: false,
});

const parseFlagValue = (raw: string | undefined, fallback: boolean): boolean => {
  if (raw === undefined) {
    return fallback;
  }
  const normalized = raw.trim().toLowerCase();
  if (TRUE_VALUES.has(normalized)) {
    return true;
  }
  if (FALSE_VALUES.has(normalized)) {
    return false;
  }
  return fallback;
};

export const resolveFeatureFlags = (env: EnvMap = process.env): FeatureFlags => {
  const resolved: FeatureFlags = {
    ENABLE_EDEN_WS: parseFlagValue(
      env.ENABLE_EDEN_WS,
      FEATURE_FLAG_DEFAULTS.ENABLE_EDEN_WS,
    ),
    ENABLE_EFFECT_RPC_POC: parseFlagValue(
      env.ENABLE_EFFECT_RPC_POC,
      FEATURE_FLAG_DEFAULTS.ENABLE_EFFECT_RPC_POC,
    ),
    ENABLE_FASTPATH_HEARTBEAT: parseFlagValue(
      env.ENABLE_FASTPATH_HEARTBEAT,
      FEATURE_FLAG_DEFAULTS.ENABLE_FASTPATH_HEARTBEAT,
    ),
    ENABLE_WASM_POC: parseFlagValue(
      env.ENABLE_WASM_POC,
      FEATURE_FLAG_DEFAULTS.ENABLE_WASM_POC,
    ),
    ENABLE_SAB_EXPERIMENT: parseFlagValue(
      env.ENABLE_SAB_EXPERIMENT,
      FEATURE_FLAG_DEFAULTS.ENABLE_SAB_EXPERIMENT,
    ),
  };
  return Object.freeze(resolved);
};

export const getFeatureFlagDefaults = (): FeatureFlags => FEATURE_FLAG_DEFAULTS;

export const isFeatureEnabled = (
  name: FeatureFlagName,
  env: EnvMap = process.env,
): boolean => resolveFeatureFlags(env)[name];


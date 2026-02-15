export type RuntimeMode = 'development' | 'production';

const isRuntimeMode = (value: string | undefined): value is RuntimeMode =>
  value === 'development' || value === 'production';

export const resolveRuntimeMode = (
  override?: RuntimeMode,
): RuntimeMode => {
  if (override) {
    return override;
  }

  const envMode = process.env.MERISTEM_RUNTIME_MODE;
  if (isRuntimeMode(envMode)) {
    return envMode;
  }

  return 'development';
};

export const isDevelopmentMode = (mode?: RuntimeMode): boolean =>
  resolveRuntimeMode(mode) === 'development';


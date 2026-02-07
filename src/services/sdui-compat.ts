export type SduiVersion = `${number}.${number}`;

type ParsedSduiVersion = Readonly<{
  major: number;
  minor: number;
}>;

type SduiFallback = 'HIDE' | 'BASIC_FALLBACK';

export type CompatResult =
  | Readonly<{
      compatible: true;
      negotiated: SduiVersion;
    }>
  | Readonly<{
      compatible: false;
      reason: 'MAJOR_MISMATCH' | 'MINOR_TOO_LOW' | 'INVALID_VERSION';
      fallback: SduiFallback;
    }>;

const SDUI_VERSION_PATTERN = /^(?<major>\d+)\.(?<minor>\d+)$/;
const DEFAULT_SDUI_VERSION: SduiVersion = '1.0';

const toSafeInteger = (raw: string): number | null => {
  const value = Number(raw);

  if (!Number.isSafeInteger(value)) {
    return null;
  }

  return value;
};

export const parseSduiVersion = (raw: string): ParsedSduiVersion | null => {
  const matched = SDUI_VERSION_PATTERN.exec(raw);

  if (!matched?.groups) {
    return null;
  }

  const major = toSafeInteger(matched.groups.major);
  const minor = toSafeInteger(matched.groups.minor);

  if (major === null || minor === null) {
    return null;
  }

  return { major, minor };
};

export const checkSduiCompat = (
  coreVersion: SduiVersion,
  pluginVersion?: SduiVersion,
): CompatResult => {
  const resolvedPluginVersion = pluginVersion ?? DEFAULT_SDUI_VERSION;
  const parsedCore = parseSduiVersion(coreVersion);
  const parsedPlugin = parseSduiVersion(resolvedPluginVersion);

  if (!parsedCore || !parsedPlugin) {
    return {
      compatible: false,
      reason: 'INVALID_VERSION',
      fallback: 'HIDE',
    };
  }

  if (parsedCore.major !== parsedPlugin.major) {
    return {
      compatible: false,
      reason: 'MAJOR_MISMATCH',
      fallback: 'HIDE',
    };
  }

  if (parsedCore.minor < parsedPlugin.minor) {
    return {
      compatible: false,
      reason: 'MINOR_TOO_LOW',
      fallback: 'BASIC_FALLBACK',
    };
  }

  return {
    compatible: true,
    negotiated: resolvedPluginVersion,
  };
};

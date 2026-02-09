export type RuntimeCheckStatus = 'pass' | 'warn' | 'fail';

export type RuntimeCheckItem = {
  name: string;
  status: RuntimeCheckStatus;
  detail: string;
};

export type RuntimeCheckReport = {
  generatedAt: string;
  ok: boolean;
  passed: number;
  warnings: number;
  failed: number;
  items: readonly RuntimeCheckItem[];
};

type RuntimeCheckInput = {
  bunVersion?: string;
  minBunVersion?: string;
  tsgoAvailable?: boolean;
  usingSupport?: boolean;
};

type VersionTuple = readonly [number, number, number];

const parseVersion = (raw: string): VersionTuple => {
  const segments = raw
    .split('.')
    .slice(0, 3)
    .map((part) => Number.parseInt(part.replace(/[^\d].*$/, ''), 10));
  const major = Number.isFinite(segments[0]) ? segments[0] : 0;
  const minor = Number.isFinite(segments[1]) ? segments[1] : 0;
  const patch = Number.isFinite(segments[2]) ? segments[2] : 0;
  return [major, minor, patch];
};

const compareVersion = (left: VersionTuple, right: VersionTuple): number => {
  for (let index = 0; index < 3; index += 1) {
    if (left[index] > right[index]) {
      return 1;
    }
    if (left[index] < right[index]) {
      return -1;
    }
  }
  return 0;
};

const detectUsingSupport = (): boolean => {
  const probe = Bun.spawnSync({
    cmd: ['bun', '-e', 'using resource = { [Symbol.dispose]() {} };'],
    stdout: 'ignore',
    stderr: 'ignore',
  });
  return probe.exitCode === 0;
};

type BunWithWhich = typeof Bun & {
  which?: (command: string) => string | null;
};

const detectTsgoAvailability = (): boolean => {
  const bun = Bun as BunWithWhich;
  if (typeof bun.which !== 'function') {
    return false;
  }
  return bun.which('tsgo') !== null;
};

const createVersionItem = (
  bunVersionRaw: string,
  minBunVersionRaw: string,
): RuntimeCheckItem => {
  const bunVersion = parseVersion(bunVersionRaw);
  const minVersion = parseVersion(minBunVersionRaw);
  const meets = compareVersion(bunVersion, minVersion) >= 0;
  return {
    name: 'bun-version',
    status: meets ? 'pass' : 'fail',
    detail: `bun=${bunVersionRaw}, required>=${minBunVersionRaw}`,
  };
};

const createUsingItem = (usingSupport: boolean): RuntimeCheckItem => ({
  name: 'explicit-resource-management',
  status: usingSupport ? 'pass' : 'fail',
  detail: usingSupport
    ? 'using/await using is available'
    : 'using/await using is unavailable',
});

const createTsgoItem = (tsgoAvailable: boolean): RuntimeCheckItem => ({
  name: 'tsgo-preview',
  status: tsgoAvailable ? 'pass' : 'warn',
  detail: tsgoAvailable
    ? 'tsgo detected for TS7 preview typecheck'
    : 'tsgo not found; typecheck:next will be skipped',
});

export const collectRuntimeCheckReport = (
  input: RuntimeCheckInput = {},
): RuntimeCheckReport => {
  const bunVersionRaw = input.bunVersion ?? Bun.version;
  const minBunVersionRaw = input.minBunVersion ?? '1.3.0';
  const usingSupport = input.usingSupport ?? detectUsingSupport();
  const tsgoAvailable = input.tsgoAvailable ?? detectTsgoAvailability();

  const items: RuntimeCheckItem[] = [
    createVersionItem(bunVersionRaw, minBunVersionRaw),
    createUsingItem(usingSupport),
    createTsgoItem(tsgoAvailable),
  ];

  const passed = items.filter((item) => item.status === 'pass').length;
  const warnings = items.filter((item) => item.status === 'warn').length;
  const failed = items.filter((item) => item.status === 'fail').length;

  return {
    generatedAt: new Date().toISOString(),
    ok: failed === 0,
    passed,
    warnings,
    failed,
    items,
  };
};

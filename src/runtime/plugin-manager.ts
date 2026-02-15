import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { join, resolve, sep } from 'path';
import { BUILD_DEFAULT_HOME } from '../generated/build-meta';
import { ensureMeristemHomeLayout, type MeristemPaths } from './paths';

type Semver = Readonly<{
  major: number;
  minor: number;
  patch: number;
}>;

type PluginManifestLike = Readonly<{
  id: string;
  entry: string;
}>;

export type PluginRegistryEntry = Readonly<{
  id: string;
  name: string;
  repo: string;
  default_ref: string;
  version: string;
  core_range: string;
  entry: string;
  checksum?: string;
  enabled_by_default: boolean;
}>;

export type PluginRegistryDocument = Readonly<{
  generated_at: string;
  plugins: readonly PluginRegistryEntry[];
}>;

export type PluginLockEntry = Readonly<{
  plugin_id: string;
  repo: string;
  resolved_ref: string;
  commit: string;
  installed_at: string;
  entry: string;
}>;

type PluginLockDocument = Readonly<{
  updated_at: string;
  plugins: readonly PluginLockEntry[];
}>;

export type PluginDoctorIssue = Readonly<{
  pluginId: string;
  issue: string;
}>;

export type PluginDoctorReport = Readonly<{
  ok: boolean;
  checked: number;
  issues: readonly PluginDoctorIssue[];
}>;

const DEFAULT_REGISTRY_SOURCE = join(BUILD_DEFAULT_HOME, 'plugins.registry.json');
const DEFAULT_CORE_VERSION = process.env.MERISTEM_CORE_VERSION ?? '0.1.0';
const textDecoder = new TextDecoder();

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

const isNonEmptyString = (value: unknown): value is string =>
  typeof value === 'string' && value.trim().length > 0;

const decodeOutput = (value: unknown): string => {
  if (typeof value === 'string') {
    return value;
  }
  if (value instanceof Uint8Array) {
    return textDecoder.decode(value);
  }
  return '';
};

const parseJson = (raw: string): unknown => {
  try {
    return JSON.parse(raw);
  } catch (error) {
    throw new Error(
      `invalid JSON: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

const toSemver = (value: string): Semver | undefined => {
  const match = value.trim().match(/^(\d+)\.(\d+)\.(\d+)$/);
  if (!match) {
    return undefined;
  }

  return {
    major: Number.parseInt(match[1] ?? '0', 10),
    minor: Number.parseInt(match[2] ?? '0', 10),
    patch: Number.parseInt(match[3] ?? '0', 10),
  };
};

const compareSemver = (left: Semver, right: Semver): number => {
  if (left.major !== right.major) {
    return left.major - right.major;
  }
  if (left.minor !== right.minor) {
    return left.minor - right.minor;
  }
  return left.patch - right.patch;
};

const satisfiesCoreRange = (coreVersion: string, range: string): boolean => {
  const normalized = range.trim();
  if (normalized.length === 0 || normalized === '*') {
    return true;
  }

  const current = toSemver(coreVersion);
  if (!current) {
    return true;
  }

  if (normalized.startsWith('^')) {
    const base = toSemver(normalized.slice(1));
    if (!base) {
      return true;
    }
    return current.major === base.major && compareSemver(current, base) >= 0;
  }

  if (normalized.startsWith('~')) {
    const base = toSemver(normalized.slice(1));
    if (!base) {
      return true;
    }
    return (
      current.major === base.major
      && current.minor === base.minor
      && compareSemver(current, base) >= 0
    );
  }

  const exact = toSemver(normalized);
  if (!exact) {
    return true;
  }
  return compareSemver(current, exact) === 0;
};

const runCommand = (cmd: readonly string[], cwd: string, task: string): string => {
  const result = Bun.spawnSync({
    cmd: [...cmd],
    cwd,
    stdout: 'pipe',
    stderr: 'pipe',
    env: process.env,
  });

  const stdout = decodeOutput(result.stdout).trim();
  const stderr = decodeOutput(result.stderr).trim();
  if (result.exitCode !== 0) {
    const details = [stdout, stderr].filter((item) => item.length > 0).join('\n');
    throw new Error(`${task} failed (exit ${result.exitCode})${details.length > 0 ? `: ${details}` : ''}`);
  }

  return stdout;
};

const runCommandAllowFailure = (
  cmd: readonly string[],
  cwd: string,
): { exitCode: number; stdout: string; stderr: string } => {
  const result = Bun.spawnSync({
    cmd: [...cmd],
    cwd,
    stdout: 'pipe',
    stderr: 'pipe',
    env: process.env,
  });

  return {
    exitCode: result.exitCode,
    stdout: decodeOutput(result.stdout).trim(),
    stderr: decodeOutput(result.stderr).trim(),
  };
};

const parseRegistryEntry = (value: unknown): PluginRegistryEntry => {
  if (!isRecord(value)) {
    throw new Error('registry entry must be an object');
  }

  if (!isNonEmptyString(value.id)) {
    throw new Error('registry entry id must be a non-empty string');
  }
  if (!isNonEmptyString(value.name)) {
    throw new Error(`registry entry ${value.id} has invalid name`);
  }
  if (!isNonEmptyString(value.repo)) {
    throw new Error(`registry entry ${value.id} has invalid repo`);
  }
  if (!isNonEmptyString(value.default_ref)) {
    throw new Error(`registry entry ${value.id} has invalid default_ref`);
  }
  if (!isNonEmptyString(value.version)) {
    throw new Error(`registry entry ${value.id} has invalid version`);
  }
  if (!isNonEmptyString(value.core_range)) {
    throw new Error(`registry entry ${value.id} has invalid core_range`);
  }
  if (!isNonEmptyString(value.entry)) {
    throw new Error(`registry entry ${value.id} has invalid entry`);
  }

  return {
    id: value.id,
    name: value.name,
    repo: value.repo,
    default_ref: value.default_ref,
    version: value.version,
    core_range: value.core_range,
    entry: value.entry,
    checksum: isNonEmptyString(value.checksum) ? value.checksum : undefined,
    enabled_by_default: value.enabled_by_default === true,
  };
};

const parseRegistryDocument = (value: unknown): PluginRegistryDocument => {
  const source = isRecord(value) && Array.isArray(value.plugins) ? value.plugins : value;
  if (!Array.isArray(source)) {
    throw new Error('registry document must be an array or object with plugins');
  }

  const plugins = source.map((entry) => parseRegistryEntry(entry));
  return {
    generated_at: new Date().toISOString(),
    plugins,
  };
};

const readRegistryFile = (path: string): PluginRegistryDocument => {
  if (!existsSync(path)) {
    throw new Error(`registry source not found: ${path}`);
  }

  const raw = readFileSync(path, 'utf-8');
  return parseRegistryDocument(parseJson(raw));
};

const readRegistryFromSource = async (source: string): Promise<PluginRegistryDocument> => {
  if (source.startsWith('http://') || source.startsWith('https://')) {
    const response = await fetch(source);
    if (!response.ok) {
      throw new Error(`failed to fetch registry: ${response.status} ${response.statusText}`);
    }
    const raw = await response.text();
    return parseRegistryDocument(parseJson(raw));
  }

  if (source.startsWith('file://')) {
    const filePath = decodeURIComponent(new URL(source).pathname);
    return readRegistryFile(filePath);
  }

  return readRegistryFile(resolve(source));
};

const getRegistrySource = (override?: string): string => {
  if (override && override.trim().length > 0) {
    return override.trim();
  }

  const envSource = process.env.MERISTEM_PLUGIN_REGISTRY_URL;
  if (envSource && envSource.trim().length > 0) {
    return envSource.trim();
  }

  return DEFAULT_REGISTRY_SOURCE;
};

const readLockFile = (paths: MeristemPaths): PluginLockDocument => {
  if (!existsSync(paths.lockFilePath)) {
    return {
      updated_at: new Date(0).toISOString(),
      plugins: [],
    };
  }

  const raw = readFileSync(paths.lockFilePath, 'utf-8');
  const parsed = parseJson(raw);
  if (!isRecord(parsed) || !Array.isArray(parsed.plugins)) {
    throw new Error(`invalid plugins lock file: ${paths.lockFilePath}`);
  }

  const plugins: PluginLockEntry[] = [];
  for (const entry of parsed.plugins) {
    if (!isRecord(entry)) {
      continue;
    }

    if (
      !isNonEmptyString(entry.plugin_id)
      || !isNonEmptyString(entry.repo)
      || !isNonEmptyString(entry.resolved_ref)
      || !isNonEmptyString(entry.commit)
      || !isNonEmptyString(entry.installed_at)
      || !isNonEmptyString(entry.entry)
    ) {
      continue;
    }

    plugins.push({
      plugin_id: entry.plugin_id,
      repo: entry.repo,
      resolved_ref: entry.resolved_ref,
      commit: entry.commit,
      installed_at: entry.installed_at,
      entry: entry.entry,
    });
  }

  return {
    updated_at:
      isNonEmptyString(parsed.updated_at) ? parsed.updated_at : new Date(0).toISOString(),
    plugins,
  };
};

const writeLockFile = (paths: MeristemPaths, lock: PluginLockDocument): void => {
  mkdirSync(paths.pluginsDir, { recursive: true, mode: 0o700 });
  writeFileSync(paths.lockFilePath, `${JSON.stringify(lock, null, 2)}\n`, 'utf-8');
};

const writeRegistryCache = (paths: MeristemPaths, registry: PluginRegistryDocument): void => {
  mkdirSync(paths.registryDir, { recursive: true, mode: 0o700 });
  writeFileSync(paths.registryCachePath, `${JSON.stringify(registry, null, 2)}\n`, 'utf-8');
};

const loadRegistryCache = (paths: MeristemPaths): PluginRegistryDocument | undefined => {
  if (!existsSync(paths.registryCachePath)) {
    return undefined;
  }
  return readRegistryFile(paths.registryCachePath);
};

const findRegistryPlugin = (
  registry: PluginRegistryDocument,
  pluginId: string,
): PluginRegistryEntry => {
  const entry = registry.plugins.find((plugin) => plugin.id === pluginId);
  if (!entry) {
    throw new Error(`plugin not found in registry: ${pluginId}`);
  }
  return entry;
};

const resolvePluginDir = (paths: MeristemPaths, pluginId: string): string =>
  join(paths.pluginsDir, pluginId);

const parseManifestLike = (raw: string): PluginManifestLike => {
  const parsed = parseJson(raw);
  if (!isRecord(parsed) || !isNonEmptyString(parsed.id) || !isNonEmptyString(parsed.entry)) {
    throw new Error('invalid plugin.json: missing id/entry');
  }
  return {
    id: parsed.id,
    entry: parsed.entry,
  };
};

const validateInstalledPlugin = (
  pluginDir: string,
  expectedPluginId: string,
  expectedEntry?: string,
): { manifest: PluginManifestLike; entryPath: string } => {
  const manifestPath = join(pluginDir, 'plugin.json');
  if (!existsSync(manifestPath)) {
    throw new Error(`plugin manifest missing: ${manifestPath}`);
  }

  const manifestRaw = readFileSync(manifestPath, 'utf-8');
  const manifest = parseManifestLike(manifestRaw);
  if (manifest.id !== expectedPluginId) {
    throw new Error(
      `plugin id mismatch at ${manifestPath}, expected=${expectedPluginId}, actual=${manifest.id}`,
    );
  }

  const resolveEntryPath = (entryValue: string): string => {
    const normalizedEntry = entryValue.replace(/^\.(?:[\\/])+/u, '');
    return resolve(pluginDir, normalizedEntry);
  };

  const expectedPrefix = `${resolve(pluginDir)}${sep}`;

  /**
   * 逻辑块：注册表入口对齐。
   * - 目的：优先使用注册表声明入口，强制统一 Core 运行入口语义。
   * - 原因：插件仓迁移期间 manifest.entry 可能滞后（如仍指向 dist），需由安装器修正。
   * - 失败路径：注册表入口不存在或越界时不做修正，继续按 manifest.entry 校验并抛错。
   */
  if (expectedEntry && expectedEntry !== manifest.entry) {
    const expectedEntryPath = resolveEntryPath(expectedEntry);
    if (expectedEntryPath.startsWith(expectedPrefix) && existsSync(expectedEntryPath)) {
      const parsed = parseJson(manifestRaw);
      if (isRecord(parsed)) {
        parsed.entry = expectedEntry;
        writeFileSync(manifestPath, `${JSON.stringify(parsed, null, 2)}\n`, 'utf-8');
        return {
          manifest: {
            ...manifest,
            entry: expectedEntry,
          },
          entryPath: expectedEntryPath,
        };
      }
    }
  }

  const entryPath = resolveEntryPath(manifest.entry);
  if (entryPath !== resolve(pluginDir) && !entryPath.startsWith(expectedPrefix)) {
    throw new Error(`plugin entry escapes root: ${manifest.entry}`);
  }

  if (!existsSync(entryPath)) {
    throw new Error(`plugin entry missing: ${entryPath}`);
  }

  return { manifest, entryPath };
};

const checkoutPlugin = (
  pluginDir: string,
  repo: string,
  targetRef: string,
  targetCommit?: string,
): void => {
  const gitDir = join(pluginDir, '.git');
  const parentDir = resolve(pluginDir, '..');
  mkdirSync(parentDir, { recursive: true, mode: 0o700 });
  if (!existsSync(gitDir)) {
    if (existsSync(pluginDir)) {
      throw new Error(`plugin directory exists but is not a git repo: ${pluginDir}`);
    }
    runCommand(['git', 'clone', repo, pluginDir], parentDir, `clone plugin ${repo}`);
  }

  runCommand(['git', '-C', pluginDir, 'fetch', '--all', '--tags', '--prune'], pluginDir, 'git fetch');
  if (targetCommit && targetCommit.trim().length > 0) {
    runCommand(['git', '-C', pluginDir, 'checkout', targetCommit], pluginDir, 'git checkout commit');
    return;
  }

  runCommand(['git', '-C', pluginDir, 'checkout', targetRef], pluginDir, 'git checkout ref');
  const headRefResult = runCommandAllowFailure(
    ['git', '-C', pluginDir, 'symbolic-ref', '--quiet', '--short', 'HEAD'],
    pluginDir,
  );
  if (headRefResult.exitCode === 0 && headRefResult.stdout.trim().length > 0) {
    runCommand(
      ['git', '-C', pluginDir, 'pull', '--ff-only', 'origin', headRefResult.stdout.trim()],
      pluginDir,
      'git pull',
    );
  }
};

const installPluginDependencies = (pluginDir: string): void => {
  runCommand(['bun', 'install'], pluginDir, 'bun install');
};

const currentCommit = (pluginDir: string): string =>
  runCommand(['git', '-C', pluginDir, 'rev-parse', 'HEAD'], pluginDir, 'resolve commit').trim();

const upsertLockEntries = (
  original: PluginLockDocument,
  entries: readonly PluginLockEntry[],
): PluginLockDocument => {
  const byId = new Map<string, PluginLockEntry>();
  for (const existing of original.plugins) {
    byId.set(existing.plugin_id, existing);
  }
  for (const next of entries) {
    byId.set(next.plugin_id, next);
  }

  return {
    updated_at: new Date().toISOString(),
    plugins: [...byId.values()].sort((left, right) =>
      left.plugin_id.localeCompare(right.plugin_id)),
  };
};

const installFromRegistry = (input: {
  paths: MeristemPaths;
  entry: PluginRegistryEntry;
  refOverride?: string;
  commitOverride?: string;
}): PluginLockEntry => {
  const { paths, entry, refOverride, commitOverride } = input;
  const pluginDir = resolvePluginDir(paths, entry.id);
  const targetRef = refOverride ?? entry.default_ref;

  checkoutPlugin(pluginDir, entry.repo, targetRef, commitOverride);
  installPluginDependencies(pluginDir);
  const validation = validateInstalledPlugin(pluginDir, entry.id, entry.entry);
  const commit = currentCommit(pluginDir);

  return {
    plugin_id: entry.id,
    repo: entry.repo,
    resolved_ref: commitOverride ?? targetRef,
    commit,
    installed_at: new Date().toISOString(),
    entry: validation.manifest.entry,
  };
};

const installFromLock = (paths: MeristemPaths, lockEntry: PluginLockEntry): PluginLockEntry => {
  const pluginDir = resolvePluginDir(paths, lockEntry.plugin_id);
  checkoutPlugin(
    pluginDir,
    lockEntry.repo,
    lockEntry.resolved_ref,
    lockEntry.commit,
  );
  installPluginDependencies(pluginDir);
  const validation = validateInstalledPlugin(pluginDir, lockEntry.plugin_id, lockEntry.entry);
  const commit = currentCommit(pluginDir);

  return {
    ...lockEntry,
    commit,
    entry: validation.manifest.entry,
    installed_at: new Date().toISOString(),
  };
};

const loadRegistry = async (input: {
  paths: MeristemPaths;
  registryUrl?: string;
  refresh?: boolean;
}): Promise<PluginRegistryDocument> => {
  const { paths, registryUrl, refresh } = input;
  if (!refresh) {
    const cache = loadRegistryCache(paths);
    if (cache) {
      return cache;
    }
  }

  const source = getRegistrySource(registryUrl);
  const registry = await readRegistryFromSource(source);
  writeRegistryCache(paths, registry);
  return registry;
};

export const refreshPluginRegistry = async (input: {
  home?: string;
  registryUrl?: string;
}): Promise<{ source: string; pluginCount: number; cachePath: string }> => {
  const paths = ensureMeristemHomeLayout(input.home);
  const source = getRegistrySource(input.registryUrl);
  const registry = await readRegistryFromSource(source);
  writeRegistryCache(paths, registry);
  return {
    source,
    pluginCount: registry.plugins.length,
    cachePath: paths.registryCachePath,
  };
};

export const listAvailablePlugins = async (input: {
  home?: string;
  registryUrl?: string;
  includeIncompatible?: boolean;
}): Promise<readonly PluginRegistryEntry[]> => {
  const paths = ensureMeristemHomeLayout(input.home);
  const registry = await loadRegistry({ paths, registryUrl: input.registryUrl });
  if (input.includeIncompatible) {
    return registry.plugins;
  }

  return registry.plugins.filter((entry) =>
    satisfiesCoreRange(DEFAULT_CORE_VERSION, entry.core_range));
};

export const listInstalledPlugins = (input: {
  home?: string;
}): readonly PluginLockEntry[] => {
  const paths = ensureMeristemHomeLayout(input.home);
  const lock = readLockFile(paths);
  return lock.plugins;
};

export const syncPlugins = async (input: {
  home?: string;
  registryUrl?: string;
  pluginId?: string;
  ref?: string;
  requiredOnly?: boolean;
}): Promise<readonly PluginLockEntry[]> => {
  const paths = ensureMeristemHomeLayout(input.home);
  const registry = await loadRegistry({ paths, registryUrl: input.registryUrl });
  const targets = input.pluginId
    ? [findRegistryPlugin(registry, input.pluginId)]
    : registry.plugins.filter((entry) =>
      input.requiredOnly ? entry.enabled_by_default : true);

  const compatibleTargets = targets.filter((entry) =>
    satisfiesCoreRange(DEFAULT_CORE_VERSION, entry.core_range));

  const installed = compatibleTargets.map((entry) =>
    installFromRegistry({
      paths,
      entry,
      refOverride: input.ref,
    }));

  if (installed.length > 0) {
    const lock = readLockFile(paths);
    writeLockFile(paths, upsertLockEntries(lock, installed));
  }

  return installed;
};

export const syncRequiredLockedPlugins = async (input: {
  home?: string;
}): Promise<readonly PluginLockEntry[]> => {
  const paths = ensureMeristemHomeLayout(input.home);
  const lock = readLockFile(paths);
  const reinstalled: PluginLockEntry[] = [];

  /**
   * 逻辑块：启动前锁文件自愈。
   * - 目的：核心进程启动前确保 lock 中声明的插件目录完整可加载。
   * - 原因：用户手动删目录或迁移机器后，lock 仍在但实体缺失会导致启动漂移。
   * - 失败路径：任何插件自愈失败立即抛错，阻断启动并给出具体插件 ID。
   */
  for (const entry of lock.plugins) {
    const pluginDir = resolvePluginDir(paths, entry.plugin_id);
    try {
      validateInstalledPlugin(pluginDir, entry.plugin_id);
      continue;
    } catch {
      const repaired = installFromLock(paths, entry);
      reinstalled.push(repaired);
    }
  }

  if (reinstalled.length > 0) {
    writeLockFile(paths, upsertLockEntries(lock, reinstalled));
  }

  return reinstalled;
};

export const updatePlugins = async (input: {
  home?: string;
  registryUrl?: string;
  pluginId?: string;
  all?: boolean;
}): Promise<readonly PluginLockEntry[]> => {
  const paths = ensureMeristemHomeLayout(input.home);
  const registry = await loadRegistry({ paths, registryUrl: input.registryUrl, refresh: true });
  const lock = readLockFile(paths);

  const targetIds: string[] = [];
  if (input.pluginId) {
    targetIds.push(input.pluginId);
  } else if (input.all) {
    targetIds.push(...lock.plugins.map((entry) => entry.plugin_id));
  } else {
    throw new Error('update requires --plugin <id> or --all');
  }

  const updated: PluginLockEntry[] = [];
  for (const pluginId of targetIds) {
    const entry = findRegistryPlugin(registry, pluginId);
    if (!satisfiesCoreRange(DEFAULT_CORE_VERSION, entry.core_range)) {
      continue;
    }
    updated.push(
      installFromRegistry({
        paths,
        entry,
      }),
    );
  }

  if (updated.length > 0) {
    writeLockFile(paths, upsertLockEntries(lock, updated));
  }

  return updated;
};

export const doctorPlugins = (input: { home?: string }): PluginDoctorReport => {
  const paths = ensureMeristemHomeLayout(input.home);
  const lock = readLockFile(paths);
  const issues: PluginDoctorIssue[] = [];

  for (const entry of lock.plugins) {
    const pluginDir = resolvePluginDir(paths, entry.plugin_id);
    try {
      const validation = validateInstalledPlugin(pluginDir, entry.plugin_id);
      if (validation.manifest.entry !== entry.entry) {
        issues.push({
          pluginId: entry.plugin_id,
          issue: `manifest entry changed (lock=${entry.entry}, runtime=${validation.manifest.entry})`,
        });
      }
      const commit = currentCommit(pluginDir);
      if (commit !== entry.commit) {
        issues.push({
          pluginId: entry.plugin_id,
          issue: `commit drift (lock=${entry.commit}, runtime=${commit})`,
        });
      }
    } catch (error) {
      issues.push({
        pluginId: entry.plugin_id,
        issue: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return {
    ok: issues.length === 0,
    checked: lock.plugins.length,
    issues,
  };
};

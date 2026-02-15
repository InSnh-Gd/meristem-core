import { mkdirSync } from 'fs';
import { isAbsolute, resolve } from 'path';
import { BUILD_DEFAULT_HOME } from '../generated/build-meta';

export const MERISTEM_HOME_ENV = 'MERISTEM_HOME';

export type HomeResolveSource = 'cli' | 'env' | 'build_default';

export type MeristemPaths = Readonly<{
  home: string;
  pluginsDir: string;
  registryDir: string;
  logsDir: string;
  dataDir: string;
  cacheDir: string;
  lockFilePath: string;
  registryCachePath: string;
}>;

export type ResolveMeristemHomeResult = Readonly<{
  home: string;
  source: HomeResolveSource;
}>;

const normalizeAbsolute = (value: string): string => {
  if (isAbsolute(value)) {
    return resolve(value);
  }

  return resolve(process.cwd(), value);
};

const resolveCandidate = (
  overrideHome?: string,
): ResolveMeristemHomeResult => {
  if (overrideHome && overrideHome.trim().length > 0) {
    return {
      home: normalizeAbsolute(overrideHome.trim()),
      source: 'cli',
    };
  }

  const envHome = process.env[MERISTEM_HOME_ENV];
  if (envHome && envHome.trim().length > 0) {
    return {
      home: normalizeAbsolute(envHome.trim()),
      source: 'env',
    };
  }

  return {
    home: normalizeAbsolute(BUILD_DEFAULT_HOME),
    source: 'build_default',
  };
};

export const resolveMeristemHome = (
  overrideHome?: string,
): ResolveMeristemHomeResult => resolveCandidate(overrideHome);

export const resolveMeristemPaths = (overrideHome?: string): MeristemPaths => {
  const { home } = resolveCandidate(overrideHome);
  const pluginsDir = resolve(home, 'plugins');
  const registryDir = resolve(home, 'registry');
  const logsDir = resolve(home, 'logs');
  const dataDir = resolve(home, 'data');
  const cacheDir = resolve(home, 'cache');
  const lockFilePath = resolve(pluginsDir, 'plugins.lock.json');
  const registryCachePath = resolve(registryDir, 'plugins.registry.json');

  return {
    home,
    pluginsDir,
    registryDir,
    logsDir,
    dataDir,
    cacheDir,
    lockFilePath,
    registryCachePath,
  };
};

/**
 * 逻辑块：统一运行时目录初始化。
 * - 目的：任何入口（源码运行/单文件二进制/CLI 子命令）都使用同一目录布局。
 * - 原因：避免各模块按 cwd 各自建目录导致路径漂移和状态分叉。
 * - 失败路径：目录不可写会在 mkdirSync 抛错，由调用方统一转译并退出非 0。
 */
export const ensureMeristemHomeLayout = (
  overrideHome?: string,
): MeristemPaths => {
  const paths = resolveMeristemPaths(overrideHome);

  mkdirSync(paths.home, { recursive: true, mode: 0o700 });
  mkdirSync(paths.pluginsDir, { recursive: true, mode: 0o700 });
  mkdirSync(paths.registryDir, { recursive: true, mode: 0o700 });
  mkdirSync(paths.logsDir, { recursive: true, mode: 0o700 });
  mkdirSync(paths.dataDir, { recursive: true, mode: 0o700 });
  mkdirSync(paths.cacheDir, { recursive: true, mode: 0o700 });

  process.env[MERISTEM_HOME_ENV] = paths.home;
  return paths;
};


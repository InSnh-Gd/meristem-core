import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { BUILD_DEFAULT_HOME } from '../generated/build-meta';
import {
  ensureMeristemHomeLayout,
  MERISTEM_HOME_ENV,
  resolveMeristemHome,
  resolveMeristemPaths,
} from '../runtime/paths';

const previousHome = process.env[MERISTEM_HOME_ENV];
const createdDirs: string[] = [];

afterEach(() => {
  if (previousHome === undefined) {
    delete process.env[MERISTEM_HOME_ENV];
  } else {
    process.env[MERISTEM_HOME_ENV] = previousHome;
  }

  while (createdDirs.length > 0) {
    const target = createdDirs.pop();
    if (target) {
      rmSync(target, { recursive: true, force: true });
    }
  }
});

describe('runtime paths', () => {
  test('resolveMeristemHome prefers cli override over env', () => {
    process.env[MERISTEM_HOME_ENV] = '/tmp/env-home';
    const resolved = resolveMeristemHome('/tmp/cli-home');
    expect(resolved.home).toBe('/tmp/cli-home');
    expect(resolved.source).toBe('cli');
  });

  test('resolveMeristemHome falls back to build default when no override', () => {
    delete process.env[MERISTEM_HOME_ENV];
    const resolved = resolveMeristemHome();
    expect(resolved.home).toBe(BUILD_DEFAULT_HOME);
    expect(resolved.source).toBe('build_default');
  });

  test('ensureMeristemHomeLayout creates required directories and exports env', () => {
    const tempHome = mkdtempSync(join(tmpdir(), 'meristem-home-'));
    createdDirs.push(tempHome);

    const paths = ensureMeristemHomeLayout(tempHome);
    expect(paths.home).toBe(tempHome);
    expect(paths.pluginsDir).toBe(join(tempHome, 'plugins'));
    expect(paths.registryDir).toBe(join(tempHome, 'registry'));
    expect(paths.logsDir).toBe(join(tempHome, 'logs'));
    expect(paths.dataDir).toBe(join(tempHome, 'data'));
    expect(paths.cacheDir).toBe(join(tempHome, 'cache'));
    expect(process.env[MERISTEM_HOME_ENV]).toBe(tempHome);

    const resolved = resolveMeristemPaths();
    expect(resolved.home).toBe(tempHome);
  });
});


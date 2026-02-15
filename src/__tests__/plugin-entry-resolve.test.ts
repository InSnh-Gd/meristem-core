import { afterEach, describe, expect, test } from 'bun:test';
import { mkdirSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import type { PluginManifest } from '@insnh-gd/meristem-shared';
import { resolvePluginEntryPath } from '../services/plugin-lifecycle';

const tempRoots: string[] = [];

const createManifest = (entry: string): PluginManifest => ({
  id: `com.meristem.test.${crypto.randomUUID().replaceAll('-', '')}`,
  name: 'Test Plugin',
  version: '1.0.0',
  tier: 'extension',
  runtime_profile: 'sandbox',
  sdui_version: '1.0',
  dependencies: [],
  entry,
  ui: {
    mode: 'SDUI',
    entry: 'ui/index.html',
  },
  ui_contract: {
    route: '/plugins/test',
    channels: [],
    default_log_level: 'info',
    stream_profile: 'balanced',
  },
  permissions: ['node:read'],
  events: [],
  exports: [],
});

afterEach(() => {
  while (tempRoots.length > 0) {
    const target = tempRoots.pop();
    if (target) {
      rmSync(target, { recursive: true, force: true });
    }
  }
});

describe('resolvePluginEntryPath', () => {
  test('resolves plugin root + relative manifest entry', () => {
    const root = join(tmpdir(), `meristem-plugin-${crypto.randomUUID()}`);
    tempRoots.push(root);
    mkdirSync(join(root, 'src'), { recursive: true });
    writeFileSync(join(root, 'src', 'index.ts'), 'export default {};\n', 'utf-8');

    const manifest = createManifest('src/index.ts');
    const result = resolvePluginEntryPath(root, manifest);

    expect(result.success).toBe(true);
    if (!result.success) {
      throw new Error(result.error);
    }
    expect(result.entryPath).toBe(join(root, 'src', 'index.ts'));
  });

  test('supports direct entry path input for compatibility', () => {
    const root = join(tmpdir(), `meristem-plugin-entry-${crypto.randomUUID()}`);
    tempRoots.push(root);
    mkdirSync(root, { recursive: true });
    const entryFile = join(root, 'plugin-entry.ts');
    writeFileSync(entryFile, 'export default {};\n', 'utf-8');

    const manifest = createManifest('src/index.ts');
    const result = resolvePluginEntryPath(entryFile, manifest);

    expect(result.success).toBe(true);
    if (!result.success) {
      throw new Error(result.error);
    }
    expect(result.entryPath).toBe(entryFile);
  });

  test('rejects path traversal in manifest entry', () => {
    const root = join(tmpdir(), `meristem-plugin-traversal-${crypto.randomUUID()}`);
    tempRoots.push(root);
    mkdirSync(join(root, 'src'), { recursive: true });
    writeFileSync(join(root, 'src', 'index.ts'), 'export default {};\n', 'utf-8');

    const manifest = createManifest('../outside.ts');
    const result = resolvePluginEntryPath(root, manifest);
    expect(result.success).toBe(false);
    if (result.success) {
      throw new Error('expected traversal validation to fail');
    }
    expect(result.error.includes('escapes plugin root')).toBe(true);
  });
});


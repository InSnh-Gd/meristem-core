import { describe, expect, it } from 'bun:test';
import type { PluginManifest } from '@insnh-gd/meristem-shared';
import {
  topologicalSort,
  validateManifest,
  validatePluginTopology,
} from '../services/manifest-validator';

const createManifest = (
  id: string,
  dependencies: string[] = [],
): PluginManifest => ({
  id,
  name: id,
  version: '1.0.0',
  tier: 'extension',
  runtime_profile: 'sandbox',
  sdui_version: '1.0',
  dependencies,
  entry: 'dist/index.js',
  ui: {
    entry: 'dist/ui.js',
    mode: 'SDUI',
    icon: 'plugin',
  },
  ui_contract: {
    route: `/p/${id.split('.').at(-1) ?? 'plugin'}`,
    channels: ['node.pulse'],
    default_log_level: 'info',
    stream_profile: 'balanced',
  },
  permissions: ['node:read'],
  events: ['node.online'],
  exports: ['service'],
});

describe('manifest validator', () => {
  it('accepts a valid manifest', () => {
    const manifest = createManifest('com.meristem.docker');
    const result = validateManifest(manifest);

    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  it('rejects missing required fields', () => {
    const manifest = createManifest('com.meristem.missing');
    const withoutName: Partial<PluginManifest> = { ...manifest };
    delete withoutName.name;

    const result = validateManifest(withoutName);

    expect(result.valid).toBe(false);
    expect(result.errors).toContainEqual(
      expect.objectContaining({
        code: 'MISSING_FIELD',
        field: 'name',
      }),
    );
  });

  it('rejects invalid permission codes', () => {
    const manifest = {
      ...createManifest('com.meristem.permission-bad'),
      permissions: ['node:read', 'node:delete'],
    };

    const result = validateManifest(manifest);

    expect(result.valid).toBe(false);
    expect(result.errors).toContainEqual(
      expect.objectContaining({
        code: 'INVALID_PERMISSION',
        field: 'permissions',
      }),
    );
  });

  it('rejects invalid tier and runtime_profile values', () => {
    const manifest = {
      ...createManifest('com.meristem.profile-bad'),
      tier: 'platform',
      runtime_profile: 'fastlane',
    };

    const result = validateManifest(manifest);

    expect(result.valid).toBe(false);
    expect(result.errors).toContainEqual(
      expect.objectContaining({
        code: 'INVALID_TIER',
        field: 'tier',
      }),
    );
    expect(result.errors).toContainEqual(
      expect.objectContaining({
        code: 'INVALID_RUNTIME_PROFILE',
        field: 'runtime_profile',
      }),
    );
  });

  it('detects circular dependencies in plugin topology', () => {
    const manifests = new Map<string, PluginManifest>([
      ['com.meristem.a', createManifest('com.meristem.a', ['com.meristem.b'])],
      ['com.meristem.b', createManifest('com.meristem.b', ['com.meristem.c'])],
      ['com.meristem.c', createManifest('com.meristem.c', ['com.meristem.a'])],
    ]);

    const result = validatePluginTopology(manifests);

    expect(result.valid).toBe(false);
    expect(result.errors).toContainEqual(
      expect.objectContaining({
        code: 'CIRCULAR_DEPENDENCY',
        field: 'dependencies',
      }),
    );
  });

  it('topologically sorts dependencies before dependents', () => {
    const coreUtils = {
      ...createManifest('com.meristem.core-utils'),
      tier: 'core',
      runtime_profile: 'hotpath',
    } satisfies PluginManifest;
    const docker = createManifest('com.meristem.docker', ['com.meristem.core-utils']);
    const monitor = createManifest('com.meristem.monitor', ['com.meristem.docker']);

    const manifests = new Map<string, PluginManifest>([
      ['com.meristem.monitor', monitor],
      ['com.meristem.docker', docker],
      ['com.meristem.core-utils', coreUtils],
    ]);

    const loadOrder = topologicalSort(manifests);

    expect(loadOrder.hasCircularDependency).toBe(false);
    expect(loadOrder.order.indexOf('com.meristem.core-utils')).toBeLessThan(
      loadOrder.order.indexOf('com.meristem.docker'),
    );
    expect(loadOrder.order.indexOf('com.meristem.docker')).toBeLessThan(
      loadOrder.order.indexOf('com.meristem.monitor'),
    );
  });

  it('rejects invalid sdui_version format', () => {
    const manifest = {
      ...createManifest('com.meristem.sdui-bad'),
      sdui_version: 'v1',
    };

    const result = validateManifest(manifest);

    expect(result.valid).toBe(false);
    expect(result.errors).toContainEqual(
      expect.objectContaining({
        code: 'SDUI_VERSION_MISMATCH',
        field: 'sdui_version',
      }),
    );
  });
});

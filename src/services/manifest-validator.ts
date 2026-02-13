import type {
  ManifestValidationError,
  ManifestValidationResult,
  PermissionCode,
  PluginLoadOrder,
  PluginManifest,
  PluginTier,
  PluginUiMode,
  RuntimeProfile,
  StreamProfilePreset,
} from '@insnh-gd/meristem-shared';

const REQUIRED_MANIFEST_FIELDS = Object.freeze([
  'id',
  'name',
  'version',
  'tier',
  'runtime_profile',
  'sdui_version',
  'dependencies',
  'entry',
  'ui',
  'ui_contract',
  'permissions',
  'events',
  'exports',
] as const);

const SDUI_VERSION_PATTERN = /^\d+\.\d+$/;
const REVERSE_DOMAIN_ID_PATTERN = /^(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/;

const VALID_TIERS = new Set<PluginTier>(['core', 'extension']);
const VALID_RUNTIME_PROFILES = new Set<RuntimeProfile>(['hotpath', 'sandbox']);
const VALID_UI_MODES = new Set<PluginUiMode>(['SDUI', 'ESM']);
const VALID_STREAM_PROFILES = new Set<StreamProfilePreset>(['realtime', 'balanced', 'conserve']);
const VALID_LOG_LEVELS = new Set(['info', 'debug']);
const VALID_PERMISSION_CODES = new Set<PermissionCode>([
  'sys:manage',
  'sys:audit',
  'node:read',
  'node:cmd',
  'node:join',
  'mfs:write',
  'nats:pub',
  'plugin:access',
]);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isNonEmptyString = (value: unknown): value is string =>
  typeof value === 'string' && value.trim().length > 0;

const isStringArray = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((item) => typeof item === 'string');

const hasOwn = (value: Record<string, unknown>, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key);

const toError = (
  code: ManifestValidationError['code'],
  message: string,
  field?: string,
  details?: unknown,
): ManifestValidationError => ({
  code,
  field,
  message,
  details,
});

/**
 * 逻辑块：Manifest 基础约束校验。
 * - 目标：把协议中的“必填 + 枚举 + 格式”要求收敛为可执行结果。
 * - 降级：未知结构不会抛异常，统一转为错误集合返回给上层。
 */
export const validateManifest = (manifest: unknown): ManifestValidationResult => {
  const errors: ManifestValidationError[] = [];

  if (!isRecord(manifest)) {
    return {
      valid: false,
      errors: [toError('INVALID_FIELD', 'Manifest must be an object', 'manifest')],
      warnings: [],
    };
  }

  for (const requiredField of REQUIRED_MANIFEST_FIELDS) {
    if (!hasOwn(manifest, requiredField)) {
      errors.push(
        toError(
          'MISSING_FIELD',
          `Missing required field: ${requiredField}`,
          requiredField,
        ),
      );
    }
  }

  if (hasOwn(manifest, 'id')) {
    if (!isNonEmptyString(manifest.id) || !REVERSE_DOMAIN_ID_PATTERN.test(manifest.id)) {
      errors.push(
        toError(
          'INVALID_FIELD',
          'id must use reverse domain notation, e.g. com.meristem.docker',
          'id',
        ),
      );
    }
  }

  if (hasOwn(manifest, 'name') && !isNonEmptyString(manifest.name)) {
    errors.push(toError('INVALID_FIELD', 'name must be a non-empty string', 'name'));
  }

  if (hasOwn(manifest, 'version') && !isNonEmptyString(manifest.version)) {
    errors.push(toError('INVALID_FIELD', 'version must be a non-empty string', 'version'));
  }

  if (hasOwn(manifest, 'tier')) {
    if (typeof manifest.tier !== 'string' || !VALID_TIERS.has(manifest.tier as PluginTier)) {
      errors.push(
        toError('INVALID_TIER', "tier must be 'core' or 'extension'", 'tier', manifest.tier),
      );
    }
  }

  if (hasOwn(manifest, 'runtime_profile')) {
    if (
      typeof manifest.runtime_profile !== 'string'
      || !VALID_RUNTIME_PROFILES.has(manifest.runtime_profile as RuntimeProfile)
    ) {
      errors.push(
        toError(
          'INVALID_RUNTIME_PROFILE',
          "runtime_profile must be 'hotpath' or 'sandbox'",
          'runtime_profile',
          manifest.runtime_profile,
        ),
      );
    }
  }

  if (hasOwn(manifest, 'sdui_version')) {
    if (!isNonEmptyString(manifest.sdui_version) || !SDUI_VERSION_PATTERN.test(manifest.sdui_version)) {
      errors.push(
        toError(
          'SDUI_VERSION_MISMATCH',
          'sdui_version must match MAJOR.MINOR format',
          'sdui_version',
          manifest.sdui_version,
        ),
      );
    }
  }

  if (hasOwn(manifest, 'dependencies') && !isStringArray(manifest.dependencies)) {
    errors.push(
      toError('INVALID_FIELD', 'dependencies must be an array of strings', 'dependencies'),
    );
  }

  if (hasOwn(manifest, 'entry') && !isNonEmptyString(manifest.entry)) {
    errors.push(toError('INVALID_FIELD', 'entry must be a non-empty string', 'entry'));
  }

  if (hasOwn(manifest, 'ui')) {
    if (!isRecord(manifest.ui)) {
      errors.push(toError('INVALID_FIELD', 'ui must be an object', 'ui'));
    } else {
      if (!hasOwn(manifest.ui, 'mode')) {
        errors.push(toError('MISSING_FIELD', 'Missing required field: ui.mode', 'ui.mode'));
      } else if (
        typeof manifest.ui.mode !== 'string'
        || !VALID_UI_MODES.has(manifest.ui.mode as PluginUiMode)
      ) {
        errors.push(
          toError('INVALID_FIELD', "ui.mode must be 'SDUI' or 'ESM'", 'ui.mode', manifest.ui.mode),
        );
      }

      if (hasOwn(manifest.ui, 'entry') && manifest.ui.entry !== undefined && !isNonEmptyString(manifest.ui.entry)) {
        errors.push(
          toError('INVALID_FIELD', 'ui.entry must be a non-empty string when provided', 'ui.entry'),
        );
      }

      if (hasOwn(manifest.ui, 'icon') && manifest.ui.icon !== undefined && !isNonEmptyString(manifest.ui.icon)) {
        errors.push(
          toError('INVALID_FIELD', 'ui.icon must be a non-empty string when provided', 'ui.icon'),
        );
      }
    }
  }

  if (hasOwn(manifest, 'ui_contract')) {
    if (!isRecord(manifest.ui_contract)) {
      errors.push(toError('INVALID_FIELD', 'ui_contract must be an object', 'ui_contract'));
    } else {
      if (!isNonEmptyString(manifest.ui_contract.route)) {
        errors.push(
          toError('INVALID_FIELD', 'ui_contract.route must be a non-empty string', 'ui_contract.route'),
        );
      }

      if (!isStringArray(manifest.ui_contract.channels)) {
        errors.push(
          toError('INVALID_FIELD', 'ui_contract.channels must be an array of strings', 'ui_contract.channels'),
        );
      }

      if (
        typeof manifest.ui_contract.default_log_level !== 'string'
        || !VALID_LOG_LEVELS.has(manifest.ui_contract.default_log_level)
      ) {
        errors.push(
          toError(
            'INVALID_FIELD',
            "ui_contract.default_log_level must be 'info' or 'debug'",
            'ui_contract.default_log_level',
            manifest.ui_contract.default_log_level,
          ),
        );
      }

      if (
        typeof manifest.ui_contract.stream_profile !== 'string'
        || !VALID_STREAM_PROFILES.has(manifest.ui_contract.stream_profile as StreamProfilePreset)
      ) {
        errors.push(
          toError(
            'INVALID_FIELD',
            "ui_contract.stream_profile must be 'realtime', 'balanced', or 'conserve'",
            'ui_contract.stream_profile',
            manifest.ui_contract.stream_profile,
          ),
        );
      }
    }
  }

  if (hasOwn(manifest, 'permissions')) {
    if (!isStringArray(manifest.permissions)) {
      errors.push(
        toError('INVALID_FIELD', 'permissions must be an array of strings', 'permissions'),
      );
    } else {
      for (const permission of manifest.permissions) {
        if (!VALID_PERMISSION_CODES.has(permission as PermissionCode)) {
          errors.push(
            toError(
              'INVALID_PERMISSION',
              `Invalid permission code: ${permission}`,
              'permissions',
              permission,
            ),
          );
        }
      }
    }
  }

  if (hasOwn(manifest, 'events') && !isStringArray(manifest.events)) {
    errors.push(toError('INVALID_FIELD', 'events must be an array of strings', 'events'));
  }

  if (hasOwn(manifest, 'exports') && !isStringArray(manifest.exports)) {
    errors.push(toError('INVALID_FIELD', 'exports must be an array of strings', 'exports'));
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings: [],
  };
};

const compareByTierAndId = (
  manifests: Map<string, PluginManifest>,
  leftId: string,
  rightId: string,
): number => {
  const rank = (id: string): number => {
    const manifest = manifests.get(id);
    if (!manifest) {
      return 1;
    }

    return manifest.tier === 'core' ? 0 : 1;
  };

  const rankDiff = rank(leftId) - rank(rightId);
  if (rankDiff !== 0) {
    return rankDiff;
  }

  return leftId.localeCompare(rightId);
};

const resolveCircularChain = (
  unresolvedNodes: Set<string>,
  graph: Map<string, Set<string>>,
): string[] | undefined => {
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const path: string[] = [];

  const dfs = (nodeId: string): string[] | undefined => {
    visiting.add(nodeId);
    path.push(nodeId);

    for (const nextId of graph.get(nodeId) ?? []) {
      if (!unresolvedNodes.has(nextId)) {
        continue;
      }

      if (visiting.has(nextId)) {
        const cycleStartIndex = path.indexOf(nextId);
        return [...path.slice(cycleStartIndex), nextId];
      }

      if (!visited.has(nextId)) {
        const detectedCycle = dfs(nextId);
        if (detectedCycle) {
          return detectedCycle;
        }
      }
    }

    path.pop();
    visiting.delete(nodeId);
    visited.add(nodeId);
    return undefined;
  };

  for (const nodeId of unresolvedNodes) {
    if (visited.has(nodeId)) {
      continue;
    }

    const cycle = dfs(nodeId);
    if (cycle) {
      return cycle;
    }
  }

  return undefined;
};

/**
 * 逻辑块：依赖拓扑排序（Kahn + core 优先）。
 * - 目标：保证“被依赖优先加载”，并在同层无依赖时优先 core tier。
 * - 降级：出现循环依赖时返回已排序前缀，并附带循环链便于排障。
 */
export const topologicalSort = (
  manifests: Map<string, PluginManifest>,
): PluginLoadOrder => {
  const inDegree = new Map<string, number>();
  const graph = new Map<string, Set<string>>();

  for (const pluginId of manifests.keys()) {
    inDegree.set(pluginId, 0);
    graph.set(pluginId, new Set());
  }

  for (const [pluginId, manifest] of manifests) {
    for (const dependencyId of manifest.dependencies) {
      if (!manifests.has(dependencyId)) {
        continue;
      }

      const downstream = graph.get(dependencyId);
      if (!downstream || downstream.has(pluginId)) {
        continue;
      }

      downstream.add(pluginId);
      inDegree.set(pluginId, (inDegree.get(pluginId) ?? 0) + 1);
    }
  }

  const readyQueue = [...inDegree.entries()]
    .filter(([, degree]) => degree === 0)
    .map(([pluginId]) => pluginId)
    .sort((leftId, rightId) => compareByTierAndId(manifests, leftId, rightId));

  const order: string[] = [];
  while (readyQueue.length > 0) {
    const pluginId = readyQueue.shift();
    if (!pluginId) {
      break;
    }

    order.push(pluginId);

    for (const dependentId of graph.get(pluginId) ?? []) {
      const nextDegree = (inDegree.get(dependentId) ?? 0) - 1;
      inDegree.set(dependentId, nextDegree);

      if (nextDegree === 0) {
        readyQueue.push(dependentId);
        readyQueue.sort((leftId, rightId) => compareByTierAndId(manifests, leftId, rightId));
      }
    }
  }

  if (order.length === manifests.size) {
    return {
      order,
      hasCircularDependency: false,
    };
  }

  const unresolvedNodes = new Set(
    [...inDegree.entries()].filter(([, degree]) => degree > 0).map(([pluginId]) => pluginId),
  );

  return {
    order,
    hasCircularDependency: true,
    circularChain: resolveCircularChain(unresolvedNodes, graph),
  };
};

export const validatePluginTopology = (
  manifests: Map<string, PluginManifest>,
): ManifestValidationResult => {
  const errors: ManifestValidationError[] = [];

  for (const [pluginId, manifest] of manifests) {
    if (manifest.id !== pluginId) {
      errors.push(
        toError(
          'INVALID_FIELD',
          `Manifest map key '${pluginId}' does not match manifest.id '${manifest.id}'`,
          'id',
          { pluginId, manifestId: manifest.id },
        ),
      );
    }

    for (const dependencyId of manifest.dependencies) {
      if (!manifests.has(dependencyId)) {
        errors.push(
          toError(
            'INVALID_FIELD',
            `Missing dependency '${dependencyId}' for plugin '${pluginId}'`,
            'dependencies',
            { pluginId, dependencyId },
          ),
        );
      }
    }
  }

  const loadOrder = topologicalSort(manifests);
  if (loadOrder.hasCircularDependency) {
    errors.push(
      toError(
        'CIRCULAR_DEPENDENCY',
        'Circular dependency detected in plugin manifests',
        'dependencies',
        loadOrder.circularChain,
      ),
    );
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings: [],
  };
};

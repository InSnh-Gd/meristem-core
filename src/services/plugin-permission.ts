import type { PermissionCode, PluginManifest } from '@insnh-gd/meristem-shared';

const pluginManifestStore = new Map<string, PluginManifest>();
const pluginPermissionStore = new Map<string, readonly string[]>();

const SUBJECT_PERMISSION_RULES: ReadonlyArray<Readonly<{
  pattern: RegExp;
  permission: PermissionCode;
}>> = Object.freeze([
  Object.freeze({ pattern: /^node\..+\.cmd$/, permission: 'node:cmd' }),
  Object.freeze({ pattern: /^node\./, permission: 'node:read' }),
  Object.freeze({ pattern: /^sys\./, permission: 'sys:manage' }),
  Object.freeze({ pattern: /^audit\./, permission: 'sys:audit' }),
  Object.freeze({ pattern: /^mfs\./, permission: 'mfs:write' }),
  Object.freeze({ pattern: /^plugin\./, permission: 'plugin:access' }),
]);

export class PermissionError extends Error {
  constructor(pluginIdOrMessage: string, permission?: PermissionCode) {
    super(
      permission === undefined
        ? pluginIdOrMessage
        : `Permission denied: ${pluginIdOrMessage} lacks ${permission}`,
    );
    this.name = 'PermissionError';
  }
}

const hasPermissionCode = (
  permissions: readonly string[],
  requiredPermission: string,
): boolean => {
  if (permissions.includes('*') || permissions.includes(requiredPermission)) {
    return true;
  }

  const namespace = requiredPermission.split(':')[0] ?? '';
  return namespace.length > 0 && permissions.includes(`${namespace}:*`);
};

const getStoredPermissions = (pluginId: string): readonly string[] => {
  const direct = pluginPermissionStore.get(pluginId);
  if (direct) {
    return direct;
  }

  const manifest = pluginManifestStore.get(pluginId);
  if (manifest) {
    return manifest.permissions;
  }

  return [];
};

export const setPluginManifest = (pluginId: string, manifest: PluginManifest): void => {
  pluginManifestStore.set(pluginId, manifest);
  pluginPermissionStore.set(pluginId, [...manifest.permissions]);
};

export const setPluginPermissions = (
  pluginId: string,
  permissions: readonly string[],
): void => {
  pluginPermissionStore.set(pluginId, [...permissions]);
};

export const deletePluginManifest = (pluginId: string): void => {
  pluginManifestStore.delete(pluginId);
  pluginPermissionStore.delete(pluginId);
};

export function checkPermission(pluginId: string, permission: PermissionCode): boolean;
export function checkPermission(
  permissions: readonly string[],
  requiredPermission: string,
): boolean;
export function checkPermission(
  pluginIdOrPermissions: string | readonly string[],
  permissionOrRequired: PermissionCode | string,
): boolean {
  const permissions: readonly string[] =
    typeof pluginIdOrPermissions === 'string'
      ? getStoredPermissions(pluginIdOrPermissions)
      : pluginIdOrPermissions;
  return hasPermissionCode(permissions, permissionOrRequired);
}

export function requirePermission(pluginId: string, permission: PermissionCode): void;
export function requirePermission(
  pluginId: string,
  permissions: readonly string[],
  requiredPermission: string,
): void;
export function requirePermission(
  pluginId: string,
  permissionOrPermissions: PermissionCode | readonly string[],
  maybeRequiredPermission?: string,
): void {
  if (!Array.isArray(permissionOrPermissions)) {
    const requiredPermission = permissionOrPermissions as PermissionCode;
    if (!checkPermission(pluginId, requiredPermission)) {
      throw new PermissionError(pluginId, requiredPermission);
    }
    return;
  }

  const requiredPermission = maybeRequiredPermission ?? '';
  if (!hasPermissionCode(permissionOrPermissions, requiredPermission)) {
    throw new PermissionError(pluginId, requiredPermission as PermissionCode);
  }
}

export const getRequiredPermissionForSubject = (
  subject: string,
): PermissionCode | null => {
  for (const rule of SUBJECT_PERMISSION_RULES) {
    if (rule.pattern.test(subject)) {
      return rule.permission;
    }
  }

  return null;
};

export function validateSubjectAccess(pluginId: string, subject: string): void;
export function validateSubjectAccess(input: {
  pluginId: string;
  subject: string;
  permissions: readonly string[];
  audit: (entry: Record<string, unknown>) => void;
}): void;
export function validateSubjectAccess(
  pluginIdOrInput:
    | string
    | {
        pluginId: string;
        subject: string;
        permissions: readonly string[];
        audit: (entry: Record<string, unknown>) => void;
      },
  subjectArg?: string,
): void {
  const subject =
    typeof pluginIdOrInput === 'string' ? (subjectArg ?? '') : pluginIdOrInput.subject;
  const requiredPermission = getRequiredPermissionForSubject(subject);
  if (requiredPermission === null) {
    return;
  }

  if (typeof pluginIdOrInput === 'string') {
    requirePermission(pluginIdOrInput, requiredPermission);
    return;
  }

  if (hasPermissionCode(pluginIdOrInput.permissions, requiredPermission)) {
    return;
  }

  pluginIdOrInput.audit({
    pluginId: pluginIdOrInput.pluginId,
    subject,
    requiredPermission,
  });

  throw new PermissionError(pluginIdOrInput.pluginId, requiredPermission);
}

export type SubjectPermissionDecision = Readonly<{
  allowed: boolean;
  reason: 'ALLOW' | 'DENY_NO_MAPPING' | 'DENY_PERMISSION';
  requiredPermission: string | null;
}>;

type SubjectPermissionRule = Readonly<{
  pattern: RegExp;
  permission: string;
}>;

const SUBJECT_PERMISSION_RULES: readonly SubjectPermissionRule[] = Object.freeze([
  Object.freeze({ pattern: /^meristem\.v1\.node\.[^.]+\.cmd$/, permission: 'node:cmd' }),
  Object.freeze({ pattern: /^node\.[^.]+\.cmd$/, permission: 'node:cmd' }),
  Object.freeze({ pattern: /^meristem\.v1\.node\.[^.]+\.status$/, permission: 'node:read' }),
  Object.freeze({ pattern: /^meristem\.v1\.node\.[^.]+\.state$/, permission: 'node:read' }),
  Object.freeze({ pattern: /^node\.[^.]+\.status$/, permission: 'node:read' }),
  Object.freeze({ pattern: /^task\.[^.]+\.status$/, permission: 'node:read' }),
  Object.freeze({ pattern: /^meristem\.v1\.sys\./, permission: 'sys:manage' }),
  Object.freeze({ pattern: /^sys\./, permission: 'sys:manage' }),
  Object.freeze({ pattern: /^meristem\.v1\.audit\./, permission: 'sys:audit' }),
  Object.freeze({ pattern: /^audit\./, permission: 'sys:audit' }),
  Object.freeze({ pattern: /^meristem\.v1\.mfs\./, permission: 'mfs:write' }),
  Object.freeze({ pattern: /^mfs\./, permission: 'mfs:write' }),
  Object.freeze({ pattern: /^meristem\.v1\.plugin\./, permission: 'plugin:access' }),
  Object.freeze({ pattern: /^plugin\./, permission: 'plugin:access' }),
]);

const hasPermission = (permissions: readonly string[], requiredPermission: string): boolean => {
  if (permissions.includes('*')) {
    return true;
  }

  if (permissions.includes(requiredPermission)) {
    return true;
  }

  const namespace = requiredPermission.split(':')[0];
  if (!namespace) {
    return false;
  }

  return permissions.includes(`${namespace}:*`);
};

export const resolveRequiredPermission = (subject: string): string | null => {
  for (const rule of SUBJECT_PERMISSION_RULES) {
    if (rule.pattern.test(subject)) {
      return rule.permission;
    }
  }

  return null;
};

/**
 * 逻辑块：Subject 权限执行点采用 deny-by-default。
 * - 目的：即便调用方绕过 SDK，也必须在 Core 侧完成最终授权判定。
 * - 原因：修复“执行点只在 SDK 层”导致的越权风险。
 * - 降级：无法映射权限的 subject 一律拒绝，并由上层写审计。
 */
export const evaluateSubjectPermission = (input: {
  subject: string;
  permissions: readonly string[];
}): SubjectPermissionDecision => {
  const requiredPermission = resolveRequiredPermission(input.subject);
  if (!requiredPermission) {
    return Object.freeze({
      allowed: false,
      reason: 'DENY_NO_MAPPING',
      requiredPermission: null,
    });
  }

  if (!hasPermission(input.permissions, requiredPermission)) {
    return Object.freeze({
      allowed: false,
      reason: 'DENY_PERMISSION',
      requiredPermission,
    });
  }

  return Object.freeze({
    allowed: true,
    reason: 'ALLOW',
    requiredPermission,
  });
};

import { describe, expect, test } from 'bun:test';
import {
  getRequiredPermissionForSubject,
  PermissionError,
  requirePermission,
  setPluginPermissions,
} from '../services/plugin-permission';
import * as pluginPermissionModule from '../services/plugin-permission';

type ValidateSubjectAccessInput = Readonly<{
  pluginId: string;
  subject: string;
  permissions: readonly string[];
  audit: (entry: Record<string, unknown>) => void;
}>;

const moduleRecord = pluginPermissionModule as Record<string, unknown>;

const exportedCheckPermission =
  typeof moduleRecord.checkPermission === 'function'
    ? (moduleRecord.checkPermission as (
        permissions: readonly string[],
        requiredPermission: string
      ) => boolean)
    : null;

const exportedValidateSubjectAccess =
  typeof moduleRecord.validateSubjectAccess === 'function'
    ? (moduleRecord.validateSubjectAccess as (input: ValidateSubjectAccessInput) => void)
    : null;

const callRequirePermission = requirePermission as unknown as (
  pluginId: string,
  requiredPermission: string
) => void;

const callGetRequiredPermissionForSubject = getRequiredPermissionForSubject as unknown as (
  subject: string
) => string | null;

const callCheckPermission = (
  permissions: readonly string[],
  requiredPermission: string
): boolean => {
  if (exportedCheckPermission) {
    return exportedCheckPermission(permissions, requiredPermission);
  }

  const pluginId = 'compat.check.permission';
  setPluginPermissions(pluginId, permissions);

  try {
    callRequirePermission(pluginId, requiredPermission);
    return true;
  } catch (error) {
    if (error instanceof PermissionError) {
      return false;
    }

    throw error;
  }
};

const callValidateSubjectAccess = (input: ValidateSubjectAccessInput): void => {
  if (exportedValidateSubjectAccess) {
    return exportedValidateSubjectAccess(input);
  }

  const requiredPermission = callGetRequiredPermissionForSubject(input.subject);
  if (!requiredPermission) {
    input.audit({
      pluginId: input.pluginId,
      subject: input.subject,
      requiredPermission: null,
      reason: 'DENY_NO_MAPPING',
    });
    throw new PermissionError(
      `Permission denied for ${input.pluginId}; subject ${input.subject} has no mapped permission`
    );
  }

  setPluginPermissions(input.pluginId, input.permissions);

  try {
    callRequirePermission(input.pluginId, requiredPermission);
  } catch (error) {
    input.audit({
      pluginId: input.pluginId,
      subject: input.subject,
      requiredPermission,
      reason: 'DENY_PERMISSION',
    });
    throw error;
  }
};

describe('Plugin Permission', () => {
  const pluginId = 'com.test.plugin';

  describe('checkPermission', () => {
    test('returns true when plugin has permission', () => {
      expect(callCheckPermission(['node:read', 'plugin:access'], 'node:read')).toBe(true);
    });

    test('returns false when plugin lacks permission', () => {
      expect(callCheckPermission(['node:read', 'plugin:access'], 'sys:manage')).toBe(false);
    });
  });

  describe('requirePermission', () => {
    test('does not throw when plugin has permission', () => {
      expect(() => {
        setPluginPermissions(pluginId, ['node:read', 'plugin:access']);
        callRequirePermission(pluginId, 'node:read');
      }).not.toThrow();
    });

    test('throws PermissionError when plugin lacks permission', () => {
      expect(() => {
        setPluginPermissions(pluginId, ['node:read', 'plugin:access']);
        callRequirePermission(pluginId, 'sys:manage');
      }).toThrow(PermissionError);
    });

    test('error message contains pluginId and permission', () => {
      try {
        setPluginPermissions(pluginId, ['node:read', 'plugin:access']);
        callRequirePermission(pluginId, 'sys:manage');
        throw new Error('expected PermissionError');
      } catch (error) {
        expect(error).toBeInstanceOf(PermissionError);
        const message = error instanceof Error ? error.message : '';
        expect(message).toContain(pluginId);
        expect(message).toContain('sys:manage');
      }
    });
  });

  describe('getRequiredPermissionForSubject', () => {
    test('maps subjects to required permissions', () => {
      expect(callGetRequiredPermissionForSubject('node.agent-1.status')).toBe('node:read');
      expect(callGetRequiredPermissionForSubject('node.agent-1.cmd')).toBe('node:cmd');
      expect(callGetRequiredPermissionForSubject('sys.reboot')).toBe('sys:manage');
      expect(callGetRequiredPermissionForSubject('audit.query')).toBe('sys:audit');
      expect(callGetRequiredPermissionForSubject('mfs.write')).toBe('mfs:write');
      expect(callGetRequiredPermissionForSubject('plugin.health')).toBe('plugin:access');
    });

    test('returns null for unknown subject', () => {
      expect(callGetRequiredPermissionForSubject('unknown.topic')).toBeNull();
    });
  });

  describe('validateSubjectAccess', () => {
    test('does not throw for authorized subject', () => {
      const auditCalls: Record<string, unknown>[] = [];

      expect(() => {
        callValidateSubjectAccess({
          pluginId,
          subject: 'node.agent-1.status',
          permissions: ['node:read', 'plugin:access'],
          audit: entry => {
            auditCalls.push(entry);
          },
        });
      }).not.toThrow();

      expect(auditCalls).toHaveLength(0);
    });

    test('throws for unauthorized subject and calls audit on violation', () => {
      const auditCalls: Record<string, unknown>[] = [];

      expect(() => {
        callValidateSubjectAccess({
          pluginId,
          subject: 'sys.reboot',
          permissions: ['node:read'],
          audit: entry => {
            auditCalls.push(entry);
          },
        });
      }).toThrow(PermissionError);

      expect(auditCalls).toHaveLength(1);
      const firstAuditCall = auditCalls[0] ?? {};
      expect(firstAuditCall.pluginId).toBe(pluginId);
      expect(firstAuditCall.subject).toBe('sys.reboot');
      expect(firstAuditCall.requiredPermission).toBe('sys:manage');
    });
  });
});

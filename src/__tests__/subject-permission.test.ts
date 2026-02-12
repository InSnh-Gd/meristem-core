import { describe, expect, it } from 'bun:test';
import {
  evaluateSubjectPermission,
  resolveRequiredPermission,
} from '../services/subject-permission-guard';

describe('subject permission guard', () => {
  it('resolves required permission for node command and status subjects', () => {
    expect(resolveRequiredPermission('node.1.cmd')).toBe('node:cmd');
    expect(resolveRequiredPermission('node.1.status')).toBe('node:read');
    expect(resolveRequiredPermission('task.1.status')).toBe('node:read');
  });

  it('denies unknown subjects by default', () => {
    const decision = evaluateSubjectPermission({
      subject: 'unknown.topic',
      permissions: ['*'],
    });

    expect(decision.allowed).toBe(false);
    expect(decision.reason).toBe('DENY_NO_MAPPING');
    expect(decision.requiredPermission).toBeNull();
  });

  it('allows known subject when matching permission exists', () => {
    const decision = evaluateSubjectPermission({
      subject: 'meristem.v1.node.agent-1.cmd',
      permissions: ['node:cmd'],
    });

    expect(decision.allowed).toBe(true);
    expect(decision.requiredPermission).toBe('node:cmd');
    expect(decision.reason).toBe('ALLOW');
  });

  it('denies known subject when required permission is missing', () => {
    const decision = evaluateSubjectPermission({
      subject: 'sys.reboot',
      permissions: ['node:read'],
    });

    expect(decision.allowed).toBe(false);
    expect(decision.requiredPermission).toBe('sys:manage');
    expect(decision.reason).toBe('DENY_PERMISSION');
  });
});

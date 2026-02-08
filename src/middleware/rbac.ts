import { Elysia } from 'elysia';
import type { AuthStore } from './auth';

/**
 * requirePermission Hook 工厂函数
 *
 * 创建一个 Elysia beforeHandle 钩子，用于检查用户是否拥有指定权限
 *
 * 权限代码定义参考 docs/standards/PLUGIN_PROTOCOL.md §2
 * - sys:manage, sys:audit
 * - node:read, node:cmd, node:join
 * - mfs:write, nats:pub, plugin:access
 *
 * 使用方式：
 * ```typescript
 * app.get('/audit-logs', handler, {
 *   beforeHandle: [requireAuth, requirePermission('sys:audit')]
 * })
 * ```
 *
 * @param perm - 所需的权限代码
 * @returns Elysia beforeHandle 钩子函数
 */
export const requirePermission = (perm: string) => {
  return (context: { set: { status?: unknown }; store: Record<string, unknown> }) => {
    const store = context.store as unknown as AuthStore;
    const user = store.user;

    if (!user) {
      context.set.status = 401;
      return {
        success: false,
        error: 'UNAUTHORIZED',
        message: 'Authentication required',
      };
    }

    const namespace = perm.includes(':') ? perm.split(':')[0] : null;
    const hasPermission =
      user.permissions.includes('*') ||
      user.permissions.includes(perm) ||
      (namespace ? user.permissions.includes(`${namespace}:*`) : false);

    if (!hasPermission) {
      context.set.status = 403;
      return {
        success: false,
        error: 'ACCESS_DENIED',
        message: `Permission '${perm}' required`,
      };
    }
  };
};

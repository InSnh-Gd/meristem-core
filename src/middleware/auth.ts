import { Elysia } from 'elysia';
import { SignJWT, jwtVerify } from 'jose';
import { createLogger } from '../utils/logger';
import type { TraceContext } from '../utils/trace-context';

/**
 * JWT Payload 类型定义
 *
 * 符合 docs/standards/API_SDK_SPEC.md §2.2 JWT 规格
 */
export interface JwtPayload {
  sub: string;
  type: 'USER' | 'NODE' | 'PLUGIN';
  permissions: string[];
  node_id?: string;
  iat: number;
  exp: number;
}

/**
 * Elysia Store 类型扩展
 *
 * 在认证成功后，将用户信息注入到 store.user
 */
export interface AuthStore {
  user: {
    id: string;
    type: 'USER' | 'NODE' | 'PLUGIN';
    permissions: string[];
    node_id?: string;
  };
}

/**
 * 从 Authorization 头提取 Bearer Token
 *
 * @param authHeader - Authorization 头的值
 * @returns JWT Token 字符串，如果格式无效则返回 null
 */
const extractBearerToken = (authHeader: string | null): string | null => {
  if (!authHeader) {
    return null;
  }

  const parts = authHeader.split(' ');
  if (parts.length !== 2 || parts[0] !== 'Bearer') {
    return null;
  }

  return parts[1];
};

const formatError = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

/**
 * 验证 JWT Token
 *
 * 使用 HS256 算法验证 JWT Token
 * 密钥从环境变量 JWT_SECRET 获取
 *
 * @param token - JWT Token 字符串
 * @returns 验证后的 Payload，如果验证失败则返回 null
 */
const verifyJwtToken = async (
  traceContext: TraceContext,
  token: string
): Promise<JwtPayload | null> => {
  const logger = createLogger(traceContext);
  const jwtSecret = process.env.JWT_SECRET;

  if (!jwtSecret) {
    logger.error('[Auth] JWT_SECRET environment variable not set');
    return null;
  }

  try {
    const secretKey = new TextEncoder().encode(jwtSecret);

    const { payload } = await jwtVerify(token, secretKey);

    if (
      !payload.sub ||
      !payload.type ||
      !payload.permissions ||
      !Array.isArray(payload.permissions) ||
      typeof payload.exp !== 'number'
    ) {
      logger.error('[Auth] Invalid JWT payload structure');
      return null;
    }

    const now = Math.floor(Date.now() / 1000);
    if (payload.exp < now) {
      logger.error('[Auth] JWT token expired');
      return null;
    }

    return payload as unknown as JwtPayload;
  } catch (error) {
    logger.error('[Auth] JWT verification failed:', {
      error: formatError(error),
    });
    return null;
  }
};

/**
 * requireAuth Elysia Hook
 *
 * 验证 JWT Token 并将用户信息注入到 store.user
 *
 * 使用方式：
 * ```typescript
 * app.get('/protected', handler, {
 *   beforeHandle: [requireAuth]
 * })
 * ```
 */
export const requireAuth = async (context: {
  headers: { authorization?: string };
  set: { status?: unknown };
  store: Record<string, unknown>;
  traceContext: TraceContext;
}) => {
  const authHeader = context.headers.authorization ?? null;
  const token = extractBearerToken(authHeader);

  if (!token) {
    context.set.status = 401;
    return {
      success: false,
      error: 'UNAUTHORIZED',
      message: 'Missing or invalid Authorization header',
    };
  }

  const payload = await verifyJwtToken(context.traceContext, token);

  if (!payload) {
    context.set.status = 401;
    return {
      success: false,
      error: 'UNAUTHORIZED',
      message: 'Invalid or expired JWT token',
    };
  }

  (context.store as unknown as AuthStore).user = {
    id: payload.sub,
    type: payload.type,
    permissions: payload.permissions,
    node_id: payload.node_id,
  };
};

import type { AuthStore } from '../middleware/auth';

type ResponseSetter = {
  status?: unknown;
};

export type AuthorizationErrorResponse = {
  success: false;
  error: 'UNAUTHORIZED' | 'ACCESS_DENIED';
};

export const ensureSuperadminAccess = (
  store: Record<string, unknown>,
  set: ResponseSetter,
): AuthorizationErrorResponse | null => {
  const authStore = store as unknown as AuthStore;
  if (!authStore.user) {
    set.status = 401;
    return {
      success: false,
      error: 'UNAUTHORIZED',
    };
  }

  if (!authStore.user.permissions.includes('*')) {
    set.status = 403;
    return {
      success: false,
      error: 'ACCESS_DENIED',
    };
  }

  return null;
};

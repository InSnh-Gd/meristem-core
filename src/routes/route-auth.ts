import type { AuthStore } from '../middleware/auth';
import { respondWithCode, type RouteErrorResponse } from './route-errors';

type ResponseSetter = {
  status?: unknown;
};

export type AuthorizationErrorResponse = RouteErrorResponse;

export const ensureSuperadminAccess = (
  store: Record<string, unknown>,
  set: ResponseSetter,
): AuthorizationErrorResponse | null => {
  const authStore = store as unknown as AuthStore;
  if (!authStore.user) {
    return respondWithCode(set, 'UNAUTHORIZED');
  }

  if (!authStore.user.permissions.includes('*')) {
    return respondWithCode(set, 'ACCESS_DENIED');
  }

  return null;
};

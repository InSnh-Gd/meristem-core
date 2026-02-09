import {
  domainErrorCodeToStatus,
  DomainErrorCode,
  toDomainError,
} from '../errors/domain-error';

type ResponseSetter = {
  status?: unknown;
};

export type RouteErrorResponse = {
  success: false;
  error: DomainErrorCode;
};

export const respondWithCode = (
  set: ResponseSetter,
  code: DomainErrorCode,
): RouteErrorResponse => {
  set.status = domainErrorCodeToStatus(code);
  return {
    success: false,
    error: code,
  };
};

export const respondWithError = (
  set: ResponseSetter,
  error: unknown,
  fallbackCode: DomainErrorCode = 'INTERNAL_ERROR',
): RouteErrorResponse => {
  const domainError = toDomainError(error, fallbackCode);
  return respondWithCode(set, domainError.code);
};

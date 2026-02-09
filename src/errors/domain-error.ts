export type DomainErrorCode =
  | 'INTERNAL_ERROR'
  | 'NOT_FOUND'
  | 'UNAUTHORIZED'
  | 'ACCESS_DENIED'
  | 'INVALID_CURSOR'
  | 'INVALID_BOOTSTRAP_TOKEN'
  | 'BOOTSTRAP_ALREADY_COMPLETED'
  | 'AUTH_INVALID_CREDENTIALS'
  | 'USER_ALREADY_EXISTS'
  | 'ROLE_ORG_MISMATCH'
  | 'ROLE_NAME_CONFLICT'
  | 'ROLE_BUILTIN_READONLY'
  | 'INVITATION_NOT_FOUND'
  | 'INVITATION_ALREADY_ACCEPTED'
  | 'INVITATION_EXPIRED'
  | 'INVALID_CALL_DEPTH'
  | 'TASK_CREATION_FAILED'
  | 'RESULT_SUBMISSION_FAILED'
  | 'TASK_NOT_FOUND'
  | 'AUDIT_BACKPRESSURE'
  | 'TRANSACTION_ABORTED';

type DomainErrorInit = {
  cause?: unknown;
  meta?: Readonly<Record<string, unknown>>;
  message?: string;
};

const HTTP_STATUS_BY_CODE: Readonly<Record<DomainErrorCode, number>> = {
  INTERNAL_ERROR: 500,
  NOT_FOUND: 404,
  UNAUTHORIZED: 401,
  ACCESS_DENIED: 403,
  INVALID_CURSOR: 400,
  INVALID_BOOTSTRAP_TOKEN: 400,
  BOOTSTRAP_ALREADY_COMPLETED: 409,
  AUTH_INVALID_CREDENTIALS: 401,
  USER_ALREADY_EXISTS: 409,
  ROLE_ORG_MISMATCH: 400,
  ROLE_NAME_CONFLICT: 409,
  ROLE_BUILTIN_READONLY: 400,
  INVITATION_NOT_FOUND: 404,
  INVITATION_ALREADY_ACCEPTED: 409,
  INVITATION_EXPIRED: 410,
  INVALID_CALL_DEPTH: 400,
  TASK_CREATION_FAILED: 500,
  RESULT_SUBMISSION_FAILED: 500,
  TASK_NOT_FOUND: 404,
  AUDIT_BACKPRESSURE: 503,
  TRANSACTION_ABORTED: 409,
};

const LEGACY_CODE_BY_MESSAGE: Readonly<Record<string, DomainErrorCode>> = {
  'bootstrap already completed': 'BOOTSTRAP_ALREADY_COMPLETED',
  'Bootstrap already completed': 'BOOTSTRAP_ALREADY_COMPLETED',
  BOOTSTRAP_ALREADY_COMPLETED: 'BOOTSTRAP_ALREADY_COMPLETED',
  AUTH_INVALID_CREDENTIALS: 'AUTH_INVALID_CREDENTIALS',
  'Invalid credentials': 'AUTH_INVALID_CREDENTIALS',
  INVALID_BOOTSTRAP_TOKEN: 'INVALID_BOOTSTRAP_TOKEN',
  'Invalid bootstrap token': 'INVALID_BOOTSTRAP_TOKEN',
  USER_ALREADY_EXISTS: 'USER_ALREADY_EXISTS',
  ROLE_ORG_MISMATCH: 'ROLE_ORG_MISMATCH',
  ROLE_NAME_CONFLICT: 'ROLE_NAME_CONFLICT',
  ROLE_BUILTIN_READONLY: 'ROLE_BUILTIN_READONLY',
  INVITATION_NOT_FOUND: 'INVITATION_NOT_FOUND',
  INVITATION_ALREADY_ACCEPTED: 'INVITATION_ALREADY_ACCEPTED',
  INVITATION_EXPIRED: 'INVITATION_EXPIRED',
  INVALID_CALL_DEPTH: 'INVALID_CALL_DEPTH',
  TASK_CREATION_FAILED: 'TASK_CREATION_FAILED',
  RESULT_SUBMISSION_FAILED: 'RESULT_SUBMISSION_FAILED',
  TRANSACTION_ABORTED: 'TRANSACTION_ABORTED',
  'Task not found': 'TASK_NOT_FOUND',
};

const describeUnknownError = (error: unknown): string => {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
};

export class DomainError extends Error {
  readonly code: DomainErrorCode;
  readonly status: number;
  readonly meta: Readonly<Record<string, unknown>>;

  constructor(code: DomainErrorCode, init: DomainErrorInit = {}) {
    super(init.message ?? code);
    this.name = 'DomainError';
    this.code = code;
    this.status = HTTP_STATUS_BY_CODE[code];
    this.meta = init.meta ?? {};
    if (init.cause !== undefined) {
      Object.defineProperty(this, 'cause', {
        configurable: true,
        enumerable: false,
        writable: true,
        value: init.cause,
      });
    }
  }
}

export const isDomainError = (error: unknown): error is DomainError =>
  error instanceof DomainError;

export const resolveLegacyDomainErrorCode = (
  error: unknown,
): DomainErrorCode | null => {
  if (!(error instanceof Error)) {
    return null;
  }
  const mapped = LEGACY_CODE_BY_MESSAGE[error.message];
  return mapped ?? null;
};

export const toDomainError = (
  error: unknown,
  fallbackCode: DomainErrorCode = 'INTERNAL_ERROR',
): DomainError => {
  if (isDomainError(error)) {
    return error;
  }

  const legacyCode = resolveLegacyDomainErrorCode(error);
  if (legacyCode) {
    return new DomainError(legacyCode, { cause: error });
  }

  return new DomainError(fallbackCode, {
    cause: error,
    meta: {
      reason: describeUnknownError(error),
    },
  });
};

export const domainErrorCodeToStatus = (
  code: DomainErrorCode,
): number => HTTP_STATUS_BY_CODE[code];

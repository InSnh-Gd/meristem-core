import { Effect } from 'effect';
import {
  DomainError,
  type DomainErrorCode,
  type DomainError as DomainErrorType,
  toDomainError,
} from '../errors/domain-error';

type DomainErrorInit = {
  cause?: unknown;
  meta?: Readonly<Record<string, unknown>>;
  message?: string;
};

export const failDomainError = (
  code: DomainErrorCode,
  init: DomainErrorInit = {},
): Effect.Effect<never, DomainErrorType> =>
  Effect.fail(new DomainError(code, init));

export const mapEffectErrorToDomain = <A, E, R>(
  program: Effect.Effect<A, E, R>,
  fallbackCode: DomainErrorCode = 'INTERNAL_ERROR',
): Effect.Effect<A, DomainErrorType, R> =>
  Effect.mapError(program, (error) => toDomainError(error, fallbackCode));


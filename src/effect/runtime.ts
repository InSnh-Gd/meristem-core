import { Cause, Effect, Exit } from 'effect';
import { type DomainErrorCode, toDomainError } from '../errors/domain-error';

const toUnknownError = (error: unknown): Error =>
  error instanceof Error ? error : new Error(String(error));

export const tryPromiseEffect = <A>(
  thunk: () => Promise<A>,
): Effect.Effect<A, Error> =>
  Effect.tryPromise({
    try: thunk,
    catch: toUnknownError,
  });

export const runEffect = async <A, E>(
  program: Effect.Effect<A, E, never>,
  fallbackCode: DomainErrorCode = 'INTERNAL_ERROR',
): Promise<A> => {
  const exit = await Effect.runPromiseExit(program);
  if (Exit.isSuccess(exit)) {
    return exit.value;
  }
  throw toDomainError(Cause.squash(exit.cause), fallbackCode);
};


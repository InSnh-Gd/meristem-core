import { expect, test } from 'bun:test';
import { Effect } from 'effect';
import {
  failDomainError,
  mapEffectErrorToDomain,
  runEffect,
  tryPromiseEffect,
} from '../effect';

test('tryPromiseEffect wraps async success', async (): Promise<void> => {
  const value = await runEffect(
    tryPromiseEffect(async () => 'ok'),
  );
  expect(value).toBe('ok');
});

test('mapEffectErrorToDomain converts unknown errors to DomainError', async (): Promise<void> => {
  const program = mapEffectErrorToDomain(
    Effect.fail(new Error('boom')),
    'TASK_CREATION_FAILED',
  );

  await expect(runEffect(program, 'INTERNAL_ERROR')).rejects.toMatchObject({
    code: 'TASK_CREATION_FAILED',
  });
});

test('failDomainError propagates explicit domain code', async (): Promise<void> => {
  const program = failDomainError('TRANSACTION_ABORTED');
  await expect(runEffect(program)).rejects.toMatchObject({
    code: 'TRANSACTION_ABORTED',
  });
});


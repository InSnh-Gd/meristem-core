import { expect, test } from 'bun:test';
import { DomainError, toDomainError } from '../errors/domain-error';

test('toDomainError keeps existing DomainError instances', (): void => {
  const domainError = new DomainError('AUTH_INVALID_CREDENTIALS');
  expect(toDomainError(domainError, 'INTERNAL_ERROR')).toBe(domainError);
});

test('toDomainError no longer maps legacy error messages', (): void => {
  const legacyMessageError = new Error('Invalid credentials');
  const mapped = toDomainError(legacyMessageError, 'INTERNAL_ERROR');

  expect(mapped.code).toBe('INTERNAL_ERROR');
  expect(mapped.meta.reason).toBe('Invalid credentials');
});

import { expect, test } from 'bun:test';
import { collectRuntimeCheckReport } from '../runtime/runtime-check';

test('collectRuntimeCheckReport returns pass when all baseline checks pass', (): void => {
  const report = collectRuntimeCheckReport({
    bunVersion: '1.3.8',
    minBunVersion: '1.3.0',
    usingSupport: true,
    tsgoAvailable: true,
  });

  expect(report.ok).toBe(true);
  expect(report.failed).toBe(0);
  expect(report.warnings).toBe(0);
});

test('collectRuntimeCheckReport emits warning when tsgo is unavailable', (): void => {
  const report = collectRuntimeCheckReport({
    bunVersion: '1.3.8',
    minBunVersion: '1.3.0',
    usingSupport: true,
    tsgoAvailable: false,
  });

  expect(report.ok).toBe(true);
  expect(report.warnings).toBe(1);
});

test('collectRuntimeCheckReport fails when bun version is below baseline', (): void => {
  const report = collectRuntimeCheckReport({
    bunVersion: '1.2.9',
    minBunVersion: '1.3.0',
    usingSupport: true,
    tsgoAvailable: true,
  });

  expect(report.ok).toBe(false);
  expect(report.failed).toBeGreaterThan(0);
});


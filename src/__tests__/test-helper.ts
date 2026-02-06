/**
 * Test Helper for Environment-Based Test Filtering
 *
 * This helper provides utilities to skip integration tests when NATS is not available.
 * Integration tests are marked with '@integration' in their test name.
 */

/**
 * Check if NATS is available for integration tests
 */
const isNatsAvailable = (): boolean => {
  return process.env.NATS_URL !== undefined && process.env.NATS_URL !== '';
};

/**
 * Skip test if NATS is not available
 * Use this in integration tests to skip them when NATS_URL is not set
 */
export const skipIfNoNats = (): void => {
  if (!isNatsAvailable()) {
    throw new Error('Skipping integration test: NATS_URL not set');
  }
};

/**
 * Check if a test name is an integration test
 */
export const isIntegrationTest = (testName: string): boolean => {
  return testName.includes('@integration');
};

/**
 * Get test filter function for environment-based filtering
 */
export const shouldRunTest = (testName: string): boolean => {
  const isIntegration = isIntegrationTest(testName);
  const hasNats = isNatsAvailable();

  if (isIntegration && !hasNats) {
    console.warn(`⏭️  Skipping integration test: ${testName}`);
    return false;
  }

  return true;
};

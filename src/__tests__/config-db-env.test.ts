import { afterEach, expect, test } from 'bun:test';
import { loadConfig } from '../config';

const originalEnv = {
  MERISTEM_DATABASE_MONGO_URI: process.env.MERISTEM_DATABASE_MONGO_URI,
  MONGO_URI: process.env.MONGO_URI,
};

const restoreEnv = (): void => {
  for (const [key, value] of Object.entries(originalEnv)) {
    if (value === undefined) {
      delete process.env[key];
      continue;
    }
    process.env[key] = value;
  }
};

afterEach((): void => {
  restoreEnv();
});

test('loadConfig falls back to MONGO_URI when MERISTEM_DATABASE_MONGO_URI is missing', (): void => {
  delete process.env.MERISTEM_DATABASE_MONGO_URI;
  process.env.MONGO_URI = 'mongodb://legacy-host:27017/legacy-db';

  const config = loadConfig();

  expect(config.database.mongo_uri).toBe('mongodb://legacy-host:27017/legacy-db');
});

test('loadConfig keeps MERISTEM_DATABASE_MONGO_URI as top priority', (): void => {
  process.env.MERISTEM_DATABASE_MONGO_URI = 'mongodb://modern-host:27017/modern-db';
  process.env.MONGO_URI = 'mongodb://legacy-host:27017/legacy-db';

  const config = loadConfig();

  expect(config.database.mongo_uri).toBe('mongodb://modern-host:27017/modern-db');
});

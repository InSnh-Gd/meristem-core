import { expect, test } from 'bun:test';
import { resolveMongoConfig } from '../db/connection';

test('resolveMongoConfig prefers override uri and dbName', (): void => {
  const config = resolveMongoConfig(
    {
      uri: 'mongodb://override-host:27017/override-db',
      dbName: 'override-name',
    },
    {
      MERISTEM_DATABASE_MONGO_URI: 'mongodb://env-host:27017/env-db',
      MERISTEM_DATABASE_MONGO_DB_NAME: 'env-name',
    },
  );

  expect(config.uri).toBe('mongodb://override-host:27017/override-db');
  expect(config.dbName).toBe('override-name');
});

test('resolveMongoConfig ignores legacy MONGO_URI fallback', (): void => {
  const config = resolveMongoConfig(
    {},
    {
      MONGO_URI: 'mongodb://legacy-host:27017/legacy-db',
    },
  );

  expect(config.uri).toBe('mongodb://localhost:27017/meristem');
  expect(config.dbName).toBe('meristem');
});

test('resolveMongoConfig infers dbName from uri path when db env is missing', (): void => {
  const config = resolveMongoConfig(
    {},
    {
      MERISTEM_DATABASE_MONGO_URI: 'mongodb://localhost:27017/team-alpha',
    },
  );

  expect(config.dbName).toBe('team-alpha');
});

test('resolveMongoConfig falls back to default dbName when uri has no db path', (): void => {
  const config = resolveMongoConfig(
    {},
    {
      MERISTEM_DATABASE_MONGO_URI: 'mongodb://localhost:27017',
    },
  );

  expect(config.dbName).toBe('meristem');
});

test('resolveMongoConfig uses MERISTEM_DATABASE_MONGO_DB_NAME when provided', (): void => {
  const config = resolveMongoConfig(
    {},
    {
      MERISTEM_DATABASE_MONGO_URI: 'mongodb://localhost:27017/ignored-name',
      MERISTEM_DATABASE_MONGO_DB_NAME: 'explicit-db',
    },
  );

  expect(config.dbName).toBe('explicit-db');
});

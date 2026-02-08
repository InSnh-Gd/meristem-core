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
      MONGO_URI: 'mongodb://legacy-host:27017/legacy-db',
      MONGO_DB_NAME: 'legacy-name',
    },
  );

  expect(config.uri).toBe('mongodb://override-host:27017/override-db');
  expect(config.dbName).toBe('override-name');
});

test('resolveMongoConfig prefers MERISTEM_DATABASE_MONGO_URI over MONGO_URI', (): void => {
  const config = resolveMongoConfig(
    {},
    {
      MERISTEM_DATABASE_MONGO_URI: 'mongodb://modern-host:27017/modern-db',
      MONGO_URI: 'mongodb://legacy-host:27017/legacy-db',
    },
  );

  expect(config.uri).toBe('mongodb://modern-host:27017/modern-db');
  expect(config.dbName).toBe('modern-db');
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

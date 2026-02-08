import { MongoClient, MongoClientOptions, Db } from 'mongodb';
import { createLogger } from '../utils/logger';
import type { TraceContext } from '../utils/trace-context';

type MongoOptions = MongoClientOptions & Record<string, unknown>;
type EnvMap = Readonly<Record<string, string | undefined>>;

type MongoConfigInput = {
  uri: string;
  dbName?: string;
  options?: MongoOptions;
};

export type MongoConfig = {
  uri: string;
  dbName: string;
  options?: MongoOptions;
};

const inferDbNameFromUri = (uri: string): string | undefined => {
  const schemeIndex = uri.indexOf('://');
  if (schemeIndex < 0) {
    return undefined;
  }

  const authorityAndPath = uri.slice(schemeIndex + 3);
  const pathIndex = authorityAndPath.indexOf('/');
  if (pathIndex < 0) {
    return undefined;
  }

  const pathAndQuery = authorityAndPath.slice(pathIndex + 1);
  const path = pathAndQuery.split('?')[0] ?? '';
  return path.length > 0 ? path : undefined;
};

export const resolveMongoConfig = (
  override: Partial<MongoConfigInput> = {},
  env: EnvMap = process.env,
): MongoConfig => {
  const uri =
    override.uri ??
    env.MERISTEM_DATABASE_MONGO_URI ??
    env.MONGO_URI ??
    'mongodb://localhost:27017/meristem';
  const dbName =
    override.dbName ??
    env.MERISTEM_DATABASE_MONGO_DB_NAME ??
    env.MONGO_DB_NAME ??
    inferDbNameFromUri(uri) ??
    'meristem';
  return { uri, dbName, options: override.options };
};

let client: MongoClient | null = null;
let db: Db | null = null;

/**
 * 使用纯函数获取或建立 Mongo 客户端，确保多次调用不会重复连接
 */
export const connectDb = async (
  traceContext: TraceContext,
  override: Partial<MongoConfigInput> = {}
): Promise<Db> => {
  if (db) {
    return db;
  }
  const config = resolveMongoConfig(override);
  client = new MongoClient(config.uri, config.options);
  await client.connect();
  db = client.db(config.dbName);
  const logger = createLogger(traceContext);
  logger.info(`[DB] 连接到 ${config.dbName}`);
  return db;
};

/**
 * 获取当前数据库实例，若尚未连接则延迟建立
 */
export const getDb = async (traceContext: TraceContext): Promise<Db> => {
  if (!db) {
    return connectDb(traceContext);
  }
  return db;
};

/**
 * 纯函数关闭连接并清理模块级状态，便于测试重置
 */
export const closeDb = async (traceContext: TraceContext): Promise<void> => {
  if (!client) {
    return;
  }
  await client.close();
  client = null;
  db = null;
  const logger = createLogger(traceContext);
  logger.info('[DB] MongoDB 连接已关闭');
};

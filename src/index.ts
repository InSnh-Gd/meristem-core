import { Elysia } from 'elysia';
import { connectDb } from './db/connection';
import { ensureDbIndexes } from './db/indexes';
import { joinRoute } from './routes/join';
import { auditRoute } from './routes/audit';
import { bootstrapRoute } from './routes/bootstrap';
import { authRoute } from './routes/auth';
import { tasksRoute } from './routes/tasks';
import { resultsRoute } from './routes/results';
import { metricsRoute } from './routes/metrics';
import { connectNats, subscribe } from './nats/connection';
import { handleHeartbeatMessage, startHeartbeatMonitor } from './services/heartbeat';
import { createPulseMessageHandler } from './services/pulse-ingest';
import { setupJetstreamLogs } from './services/jetstream-setup';
import { createLogger } from './utils/logger';
import { createTraceContext } from './utils/trace-context';
import { loadConfig } from './config';
import { wsRoute } from './routes/ws';
import { traceMiddleware } from './middleware/trace';
import { usersRoute } from './routes/users';
import { rolesRoute } from './routes/roles';

export type AppConfig = {
  port?: number;
  nodeId?: string;
  healthRoute?: string;
  metadata?: Record<string, unknown>;
};

const DEFAULT_PORT = 3000;

const resolvePort = (config: AppConfig): number => {
  const envPort = process.env.PORT ? Number(process.env.PORT) : undefined;
  return config.port ?? envPort ?? DEFAULT_PORT;
};

/**
 * 创建 Elysia 实例的纯函数，便于测试和复用
 */
export const createApp = (config: AppConfig = {}): Elysia => {
  const route = config.healthRoute ?? '/health';
  const app = new Elysia();
  app.get(route, () => ({
    status: 'ok',
    metadata: config.metadata ?? {},
  }));
  return app;
};

/**
 * 纯函数化启动逻辑，确保在不同环境中也能一致配置
 */
export const startApp = async (config: AppConfig = {}): Promise<Elysia> => {
  const port = resolvePort(config);
  const coreConfig = loadConfig();
  const initTraceContext = createTraceContext({
    traceId: 'init',
    nodeId: 'core',
    source: 'bootstrap',
  });
  const initLogger = createLogger(initTraceContext);
  const nodeId = config.nodeId ?? 'core';
  const natsTraceContext = createTraceContext({
    traceId: 'system',
    nodeId,
    source: 'nats',
  });
  const heartbeatTraceContext = createTraceContext({
    traceId: 'system',
    nodeId,
    source: 'heartbeat',
  });

  const db = await connectDb(initTraceContext, {
    uri: coreConfig.database.mongo_uri,
  });
  await ensureDbIndexes(db, initTraceContext);

  await connectNats(initTraceContext);

  await setupJetstreamLogs(initTraceContext);

  await subscribe(natsTraceContext, 'meristem.v1.hb.>', async (msg) => {
    await handleHeartbeatMessage(db, heartbeatTraceContext, msg);
  });

  const pulseHandler = createPulseMessageHandler();
  await subscribe(natsTraceContext, 'meristem.v1.sys.pulse', async (msg) => {
    await pulseHandler(heartbeatTraceContext, msg);
  });

  await startHeartbeatMonitor(db, heartbeatTraceContext);

  const app = createApp(config);
  app.use(traceMiddleware());
  joinRoute(app, db);
  auditRoute(app, db);
  bootstrapRoute(app, db);
  authRoute(app, db);
  usersRoute(app, db);
  rolesRoute(app, db);
  tasksRoute(app, db);
  resultsRoute(app, db);
  metricsRoute(app);
  wsRoute(app, { wsPath: coreConfig.server.ws_path });

  app.listen({ port });
  initLogger.info(`[Core] meristem-core listening on port ${port}`);
  return app;
};

if (import.meta.main) {
  void startApp();
}

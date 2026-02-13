import { Elysia } from 'elysia';
import { closeDb, connectDb } from './db/connection';
import { ensureDbIndexes } from './db/indexes';
import { joinRoute } from './routes/join';
import { auditRoute } from './routes/audit';
import { bootstrapRoute } from './routes/bootstrap';
import { authRoute } from './routes/auth';
import { tasksRoute } from './routes/tasks';
import { resultsRoute } from './routes/results';
import { metricsRoute } from './routes/metrics';
import { closeNats, connectNats, subscribe } from './nats/connection';
import {
  handleHeartbeatMessage,
  startHeartbeatMonitor,
  stopHeartbeatMonitor,
} from './services/heartbeat';
import { createPulseMessageHandler } from './services/pulse-ingest';
import { setupJetstreamLogs } from './services/jetstream-setup';
import { createLogger } from './utils/logger';
import { createTraceContext } from './utils/trace-context';
import { loadConfig } from './config';
import { resolveFeatureFlags } from './config/feature-flags';
import { wsRoute } from './routes/ws';
import { traceMiddleware } from './middleware/trace';
import { usersRoute } from './routes/users';
import { rolesRoute } from './routes/roles';
import { createPluginRoutes } from './routes/plugins';
import { TraceAggregator } from './services/trace-aggregator';
import { createShutdownLifecycle } from './runtime/shutdown-lifecycle';
import { startAuditPipeline, stopAuditPipeline } from './services/audit-pipeline';

export type AppConfig = {
  port?: number;
  nodeId?: string;
  healthRoute?: string;
  metadata?: Record<string, unknown>;
  installSignalHandlers?: boolean;
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
  const flags = resolveFeatureFlags();
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
  await startAuditPipeline(db, initTraceContext);

  await connectNats(initTraceContext);

  await setupJetstreamLogs(initTraceContext);

  const traceAggregator = new TraceAggregator(await connectNats(initTraceContext));

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
  app.use(createPluginRoutes(db));
  metricsRoute(app);
  wsRoute(app, {
    wsPath: coreConfig.server.ws_path,
    enableEdenSubscribe: flags.ENABLE_EDEN_WS,
  });

  app.listen({ port });
  const shutdown = createShutdownLifecycle(initLogger);
  shutdown.addTask('heartbeat-monitor', () => {
    stopHeartbeatMonitor(heartbeatTraceContext);
  });
  shutdown.addTask('trace-aggregator', () => {
    traceAggregator.stop();
  });
  shutdown.addTask('audit-pipeline', async () => {
    await stopAuditPipeline();
  });
  shutdown.addTask('nats-connection', async () => {
    await closeNats(natsTraceContext);
  });
  shutdown.addTask('mongo-connection', async () => {
    await closeDb(initTraceContext);
  });
  if (config.installSignalHandlers ?? import.meta.main) {
    shutdown.installSignalHandlers();
  }

  initLogger.info(`[Core] meristem-core listening on port ${port}`);
  initLogger.info(`[Runtime] feature flags=${JSON.stringify(flags)}`);
  return app;
};

if (import.meta.main) {
  void startApp();
}

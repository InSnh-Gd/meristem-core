import { Elysia, t } from 'elysia';
import { cp, mkdir, mkdtemp, readFile, rm } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import type { Db } from 'mongodb';
import type { Subscription } from 'nats';
import { PLUGINS_COLLECTION } from '../db/collections';
import {
  parseManifest,
  loadPlugin,
  unloadPlugin,
  initPlugin,
  startPlugin,
  stopPlugin,
  reloadPlugin,
  destroyPlugin,
  getPluginInstance,
  getPluginRegistry,
  computeLoadOrder,
  validateAllPlugins,
  loadAllPluginsInOrder,
} from '../services/plugin-lifecycle';
import { PluginIsolateManager } from '../services/plugin-isolate';
import { subscribe } from '../nats/connection';
import { createTraceContext } from '../utils/trace-context';
import { resolveMeristemPaths } from '../runtime/paths';
import { isDevelopmentMode } from '../runtime/mode';

type ErrorResponse = {
  error: string;
  details?: unknown;
};

const isolateManager = new PluginIsolateManager();
const subscriptionsByPlugin = new Map<string, Subscription[]>();

const resolvePluginBasePath = (): string => {
  const overridePath = process.env.MERISTEM_PLUGIN_BASE_PATH;
  if (overridePath && overridePath.trim().length > 0) {
    return overridePath.trim();
  }

  if (isDevelopmentMode()) {
    return '/plugins';
  }

  return resolveMeristemPaths().pluginsDir;
};

const resolvePluginRootPath = (pluginId: string): string =>
  join(resolvePluginBasePath(), pluginId);

const toErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

const createErrorResponse = (
  status: number,
  error: string,
  details?: unknown
): { status: number; body: ErrorResponse } => ({
  status,
  body: details === undefined ? { error } : { error, details },
});

const buildStatePayload = (pluginId: string) => {
  const instance = getPluginInstance(pluginId);
  if (!instance) {
    return undefined;
  }

  const now = Date.now();
  let uptime = 0;
  if (instance.started_at instanceof Date) {
    const end =
      instance.stopped_at instanceof Date ? instance.stopped_at.getTime() : now;
    uptime = Math.max(0, end - instance.started_at.getTime());
  }

  return {
    pluginId,
    state: instance.state,
    uptime,
    error: instance.error,
  };
};

/**
 * 逻辑块：插件事件订阅生命周期绑定。
 * - 目的：在 start 阶段统一挂载 manifest.events 声明的 NATS 订阅。
 * - 原因：路由层需要对插件运行状态与总线订阅状态保持一致。
 * - 失败路径：任一主题订阅失败时回滚已创建订阅并抛错，由上层转译为 400/500。
 */
const subscribeManifestEvents = async (
  pluginId: string,
  events: readonly string[]
): Promise<void> => {
  if (events.length === 0) {
    return;
  }

  const traceContext = createTraceContext({
    traceId: `plugin-start-${pluginId}`,
    source: 'plugins-route',
    nodeId: 'core',
  });

  const active = subscriptionsByPlugin.get(pluginId) ?? [];
  const created: Subscription[] = [];

  try {
    for (const eventSubject of events) {
      const subscription = await subscribe(traceContext, eventSubject, async () => {
        return;
      });
      created.push(subscription);
    }
    subscriptionsByPlugin.set(pluginId, [...active, ...created]);
  } catch (error) {
    for (const sub of created) {
      sub.unsubscribe();
    }
    throw error;
  }
};

const unsubscribeManifestEvents = (pluginId: string): void => {
  const subscriptions = subscriptionsByPlugin.get(pluginId);
  if (!subscriptions) {
    return;
  }

  for (const subscription of subscriptions) {
    subscription.unsubscribe();
  }
  subscriptionsByPlugin.delete(pluginId);
};

const parseZipFile = (body: unknown): File | undefined => {
  if (typeof body !== 'object' || body === null) {
    return undefined;
  }

  const payload = body as Record<string, unknown>;
  const fileCandidate = payload.file ?? payload.zip ?? payload.plugin;
  return fileCandidate instanceof File ? fileCandidate : undefined;
};

const lifecycleManager = {
  async transition(pluginId: string, targetState: 'INITIALIZING' | 'STARTING' | 'DESTROYED') {
    if (targetState === 'INITIALIZING') {
      return initPlugin(pluginId);
    }
    if (targetState === 'STARTING') {
      return startPlugin(pluginId);
    }
    return destroyPlugin(pluginId);
  },
  async stopPlugin(pluginId: string, timeoutMs: number) {
    return stopPlugin(pluginId, timeoutMs);
  },
};

export function createPluginRoutes(db: Db) {
  return new Elysia({ prefix: '/api/v1/plugins' })
    .get('/', async () => {
      const plugins = await db
        .collection(PLUGINS_COLLECTION)
        .find({})
        .toArray();
      return plugins;
    })

    .get('/:id', async ({ params, set }) => {
      const plugin = await db
        .collection(PLUGINS_COLLECTION)
        .findOne({ plugin_id: params.id });

      if (!plugin) {
        set.status = 404;
        return { error: 'Plugin not found' };
      }

      return plugin;
    })

    .post(
      '/',
      async ({ body, set }) => {
        const { manifest_json, plugin_path } = body;
        const parseResult = parseManifest(manifest_json);

        if (!parseResult.success) {
          set.status = 400;
          return {
            error: 'Invalid manifest',
            details: parseResult.errors,
          };
        }

        const loadResult = await loadPlugin(
          db,
          parseResult.manifest,
          plugin_path || resolvePluginRootPath(parseResult.manifest.id)
        );

        if (!loadResult.success) {
          set.status = 400;
          return { error: loadResult.error };
        }

        return {
          success: true,
          plugin_id: parseResult.manifest.id,
          manifest: parseResult.manifest,
        };
      },
      {
        body: t.Object({
          manifest_json: t.String(),
          plugin_path: t.Optional(t.String()),
        }),
      }
    )

    .delete('/:id', async ({ params, set }) => {
      const result = await unloadPlugin(db, params.id);

      if (!result.success) {
        set.status = result.error?.includes('not found') ? 404 : 400;
        return { error: result.error };
      }

      return { success: true, plugin_id: params.id };
    })

    .post('/:id/init', async ({ params, set }) => {
      const current = getPluginInstance(params.id);
      if (!current) {
        const response = createErrorResponse(404, 'Plugin not found');
        set.status = response.status;
        return response.body;
      }

      try {
        const result = await lifecycleManager.transition(params.id, 'INITIALIZING');
        if (!result.success) {
          set.status = 400;
          return { error: result.error };
        }

        return {
          success: true,
          ...(buildStatePayload(params.id) ?? { pluginId: params.id, state: 'INITIALIZING', uptime: 0 }),
        };
      } catch (error) {
        const response = createErrorResponse(500, 'Failed to initialize plugin', toErrorMessage(error));
        set.status = response.status;
        return response.body;
      }
    })

    .post('/:id/start', async ({ params, set }) => {
      const instance = getPluginInstance(params.id);
      if (!instance) {
        const response = createErrorResponse(404, 'Plugin not found');
        set.status = response.status;
        return response.body;
      }

      try {
        const result = await lifecycleManager.transition(params.id, 'STARTING');
        if (!result.success) {
          set.status = 400;
          return { error: result.error };
        }

        await subscribeManifestEvents(params.id, instance.manifest.events);

        return {
          success: true,
          ...(buildStatePayload(params.id) ?? { pluginId: params.id, state: 'RUNNING', uptime: 0 }),
        };
      } catch (error) {
        const response = createErrorResponse(400, 'Failed to start plugin', toErrorMessage(error));
        set.status = response.status;
        return response.body;
      }
    })

    .post('/:id/stop', async ({ params, set }) => {
      const instance = getPluginInstance(params.id);
      if (!instance) {
        const response = createErrorResponse(404, 'Plugin not found');
        set.status = response.status;
        return response.body;
      }

      try {
        const result = await lifecycleManager.stopPlugin(params.id, 3000);
        if (!result.success) {
          set.status = 400;
          return { error: result.error };
        }

        unsubscribeManifestEvents(params.id);
        await isolateManager.destroyIsolate(params.id);

        return {
          success: true,
          ...(buildStatePayload(params.id) ?? { pluginId: params.id, state: instance.state, uptime: 0 }),
          warning: result.error,
        };
      } catch (error) {
        const response = createErrorResponse(500, 'Failed to stop plugin', toErrorMessage(error));
        set.status = response.status;
        return response.body;
      }
    })

    .post('/:id/reload', async ({ params, set }) => {
      const result = await reloadPlugin(params.id);

      if (!result.success) {
        set.status = 400;
        return { error: result.error };
      }

      return {
        success: true,
        plugin_id: params.id,
        message: 'Plugin reloaded successfully',
      };
    })

    .post('/:id/destroy', async ({ params, set }) => {
      const instance = getPluginInstance(params.id);
      if (!instance) {
        const response = createErrorResponse(404, 'Plugin not found');
        set.status = response.status;
        return response.body;
      }

      try {
        const result = await lifecycleManager.transition(params.id, 'DESTROYED');
        if (!result.success) {
          set.status = 400;
          return { error: result.error };
        }

        unsubscribeManifestEvents(params.id);
        await isolateManager.destroyIsolate(params.id);

        return { success: true, pluginId: params.id, state: 'DESTROYED', uptime: 0 };
      } catch (error) {
        const response = createErrorResponse(500, 'Failed to destroy plugin', toErrorMessage(error));
        set.status = response.status;
        return response.body;
      }
    })

    .get('/:id/state', ({ params, set }) => {
      const payload = buildStatePayload(params.id);
      if (!payload) {
        const response = createErrorResponse(404, 'Plugin not found');
        set.status = response.status;
        return response.body;
      }
      return payload;
    })

    .post('/install-zip', async ({ body, set }) => {
      const zipFile = parseZipFile(body);
      if (!zipFile) {
        const response = createErrorResponse(
          400,
          'Missing zip file. Expected multipart/form-data field: file'
        );
        set.status = response.status;
        return response.body;
      }

      if (!zipFile.name.toLowerCase().endsWith('.zip')) {
        const response = createErrorResponse(400, 'Uploaded file must be a .zip archive');
        set.status = response.status;
        return response.body;
      }

      const tempDir = await mkdtemp(join(tmpdir(), 'meristem-plugin-'));
      const zipPath = join(tempDir, zipFile.name);
      const extractPath = join(tempDir, 'extracted');

      try {
        await Bun.write(zipPath, zipFile);
        await mkdir(extractPath, { recursive: true });

        const unzipProcess = Bun.spawn(['unzip', '-o', zipPath, '-d', extractPath], {
          stdout: 'pipe',
          stderr: 'pipe',
        });
        const unzipCode = await unzipProcess.exited;
        if (unzipCode !== 0) {
          const stderr = await new Response(unzipProcess.stderr).text();
          const response = createErrorResponse(400, 'Failed to extract zip archive', stderr.trim());
          set.status = response.status;
          return response.body;
        }

        const manifestPath = join(extractPath, 'plugin.json');
        const manifestJson = await readFile(manifestPath, 'utf-8');
        const parseResult = parseManifest(manifestJson);
        if (!parseResult.success) {
          const response = createErrorResponse(400, 'Invalid manifest', parseResult.errors);
          set.status = response.status;
          return response.body;
        }

        const pluginInstallPath = resolvePluginRootPath(parseResult.manifest.id);
        await mkdir(pluginInstallPath, { recursive: true });
        await cp(extractPath, pluginInstallPath, { recursive: true, force: true });

        const loadResult = await loadPlugin(db, parseResult.manifest, pluginInstallPath);
        if (!loadResult.success) {
          const response = createErrorResponse(400, loadResult.error ?? 'Failed to load plugin');
          set.status = response.status;
          return response.body;
        }

        return {
          success: true,
          pluginId: parseResult.manifest.id,
          manifest: parseResult.manifest,
        };
      } catch (error) {
        const message = toErrorMessage(error);
        if (message.includes('ENOENT') && message.includes('plugin.json')) {
          const response = createErrorResponse(400, 'plugin.json not found in zip archive');
          set.status = response.status;
          return response.body;
        }

        const response = createErrorResponse(500, 'Failed to install plugin zip', message);
        set.status = response.status;
        return response.body;
      } finally {
        await rm(tempDir, { recursive: true, force: true });
      }
    })

    .get('/registry/status', () => {
      const registry = getPluginRegistry();
      const status: Record<
        string,
        { state: string; version: string; tier: string }
      > = {};

      for (const [id, instance] of registry) {
        status[id] = {
          state: instance.state,
          version: instance.manifest.version,
          tier: instance.manifest.tier,
        };
      }

      return status;
    })

    .get('/topology/order', () => {
      const order = computeLoadOrder();
      return order;
    })

    .get('/topology/validate', () => {
      const validation = validateAllPlugins();
      return validation;
    })

    .post(
      '/batch/load',
      async ({ body, set }) => {
        const { manifests, base_path } = body;
        const parsedManifests = [];
        const parseErrors = [];

        for (const json of manifests) {
          const parseResult = parseManifest(json);
          if (parseResult.success) {
            parsedManifests.push(parseResult.manifest);
          } else {
            parseErrors.push({
              manifest: json.substring(0, 100),
              errors: parseResult.errors,
            });
          }
        }

        if (parseErrors.length > 0) {
          set.status = 400;
          return {
            error: 'Some manifests failed to parse',
            parse_errors: parseErrors,
          };
        }

        const result = await loadAllPluginsInOrder(
          db,
          parsedManifests,
          base_path || resolvePluginBasePath()
        );

        return result;
      },
      {
        body: t.Object({
          manifests: t.Array(t.String()),
          base_path: t.Optional(t.String()),
        }),
      }
    );
}

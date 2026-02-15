import type { Db } from 'mongodb';
import { existsSync, statSync } from 'fs';
import { extname, isAbsolute, join, resolve, sep } from 'path';
import type {
  PluginManifest,
  ManifestValidationResult,
  PluginLoadOrder,
  PluginMessage,
  PluginInvokeRequest,
  PluginInvokeResponse,
} from '@insnh-gd/meristem-shared';
import { PluginMessageType } from '@insnh-gd/meristem-shared';
import type { Msg, Subscription } from 'nats';
import {
  validateManifest,
  validatePluginTopology,
  topologicalSort,
} from './manifest-validator';
import type { PluginDocument } from '../db/collections';
import { PLUGINS_COLLECTION } from '../db/collections';
import { PluginIsolateManager } from './plugin-isolate';
import {
  MessageBridge,
  PluginContextBridge,
  EventBridge,
  type PluginContext,
} from './plugin-bridge';
import { HealthMonitor, type HealthStatus } from './plugin-health';
import { createTraceContext, type TraceContext } from '../utils/trace-context';
import { subscribe as subscribeNats } from '../nats/connection';

export type LifecycleState =
  | 'LOADED'
  | 'INITIALIZING'
  | 'INIT_ERROR'
  | 'STARTING'
  | 'START_ERROR'
  | 'RUNNING'
  | 'RELOADING'
  | 'STOPPING'
  | 'STOPPED'
  | 'DESTROYED';

export type PluginLifecycleState = LifecycleState;

export type PluginLifecycle = {
  manifest: PluginManifest;
  state: LifecycleState;
  config: Record<string, unknown>;
  entryPath: string;
  isolateId: string;
  configVersion: number;
  worker?: Worker;
  oldWorker?: Worker;
  newWorker?: Worker;
  port?: MessagePort;
  context?: PluginContext;
  error?: string;
  started_at?: Date;
  stopped_at?: Date;
  eventUnsubscribe?: () => void;
  bridgeUnsubscribe?: () => void;
};

export type PluginInstance = PluginLifecycle;

export type PluginLifecycleManagerOptions = {
  registry?: Map<string, PluginLifecycle>;
  isolateManager?: PluginIsolateManager;
  messageBridge?: MessageBridge;
  contextBridge?: PluginContextBridge;
  eventBridge?: EventBridge;
  healthMonitor?: HealthMonitor;
  natsSubscribe?: typeof subscribeNats;
  createPluginTraceContext?: (pluginId: string) => TraceContext;
};

export type InvokePluginMethodResult =
  | { success: true; data: unknown }
  | { success: false; error: string };

type TransitionPair = `${LifecycleState}->${LifecycleState}`;

const VALID_TRANSITIONS = new Set<TransitionPair>([
  'LOADED->INITIALIZING',
  'INITIALIZING->STARTING',
  'INITIALIZING->INIT_ERROR',
  'STARTING->RUNNING',
  'STARTING->START_ERROR',
  'RUNNING->STOPPING',
  'STOPPING->STOPPED',
  'STOPPED->DESTROYED',
]);

const STOP_TIMEOUT_MS = 3000;
const RELOAD_STARTUP_TIMEOUT_MS = 5000;

const textDecoder = new TextDecoder();

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

const isInvokeRequest = (value: unknown): value is PluginInvokeRequest => {
  if (!isRecord(value)) {
    return false;
  }

  return typeof value.method === 'string' && 'params' in value;
};

const isInvokeResponse = (value: unknown): value is PluginInvokeResponse => {
  if (!isRecord(value) || typeof value.success !== 'boolean') {
    return false;
  }

  if (!value.success) {
    if (!isRecord(value.error)) {
      return false;
    }

    return (
      typeof value.error.code === 'string' &&
      typeof value.error.message === 'string'
    );
  }

  return true;
};

const createLifecycleMessage = (input: {
  id?: string;
  pluginId: string;
  type: PluginMessageType;
  payload?: unknown;
}): PluginMessage => ({
  id: input.id ?? crypto.randomUUID(),
  type: input.type,
  pluginId: input.pluginId,
  timestamp: Date.now(),
  payload: input.payload,
});

const decodeNatsPayload = (msg: Msg): unknown => {
  if (msg.data.length === 0) {
    return undefined;
  }

  const raw = textDecoder.decode(msg.data);

  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
};

export class PluginLifecycleManager {
  private readonly registry: Map<string, PluginLifecycle>;
  private readonly isolateManager: PluginIsolateManager;
  private readonly messageBridge: MessageBridge;
  private readonly contextBridge: PluginContextBridge;
  private readonly eventBridge: EventBridge;
  private readonly healthMonitor: HealthMonitor;
  private readonly natsSubscribe: typeof subscribeNats;
  private readonly createPluginTraceContext: (pluginId: string) => TraceContext;

  constructor(options: PluginLifecycleManagerOptions = {}) {
    this.registry = options.registry ?? new Map<string, PluginLifecycle>();
    this.isolateManager = options.isolateManager ?? new PluginIsolateManager();
    this.messageBridge = options.messageBridge ?? new MessageBridge();
    this.contextBridge = options.contextBridge ?? new PluginContextBridge();
    this.eventBridge = options.eventBridge ?? new EventBridge();
    this.healthMonitor = options.healthMonitor ?? new HealthMonitor();
    this.natsSubscribe = options.natsSubscribe ?? subscribeNats;
    this.createPluginTraceContext =
      options.createPluginTraceContext ??
      ((pluginId: string) =>
        createTraceContext({
          nodeId: 'core',
          source: `plugin.lifecycle.${pluginId}`,
        }));
  }

  public manage(pluginId: string): PluginLifecycle {
    const lifecycle = this.registry.get(pluginId);
    if (!lifecycle) {
      throw new Error(`Plugin ${pluginId} is not managed`);
    }

    return lifecycle;
  }

  public getState(pluginId: string): LifecycleState {
    return this.manage(pluginId).state;
  }

  public getHealth(pluginId: string): HealthStatus {
    return this.healthMonitor.getHealth(pluginId);
  }

  public isResponsive(pluginId: string): boolean {
    return this.healthMonitor.isResponsive(pluginId);
  }

  /**
   * 逻辑块：统一生命周期状态跃迁入口。
   * - 目的：集中校验状态机规则并执行与目标阶段绑定的副作用。
   * - 原因：避免上层路由绕过约束导致无效状态或资源泄漏。
   * - 失败路径：非法跃迁直接抛错；Hook 失败落入 *_ERROR 终态并向上抛错。
   */
  public async transition(pluginId: string, toState: LifecycleState): Promise<void> {
    const lifecycle = this.manage(pluginId);
    const fromState = lifecycle.state;
    const key = `${fromState}->${toState}` as TransitionPair;

    if (!VALID_TRANSITIONS.has(key)) {
      throw new Error(`Invalid lifecycle transition: ${key}`);
    }

    if (fromState === 'LOADED' && toState === 'INITIALIZING') {
      lifecycle.state = 'INITIALIZING';
      try {
        const context = this.contextBridge.createContext(pluginId, lifecycle.manifest);
        lifecycle.context = context;
        await this.executeOnInit(pluginId, context);
        lifecycle.state = 'STARTING';
        lifecycle.error = undefined;
        return;
      } catch (error) {
        lifecycle.state = 'INIT_ERROR';
        lifecycle.error = error instanceof Error ? error.message : String(error);
        throw error;
      }
    }

    if (fromState === 'INITIALIZING' && toState === 'STARTING') {
      lifecycle.state = 'STARTING';
      lifecycle.error = undefined;
      return;
    }

    if (fromState === 'STARTING' && toState === 'RUNNING') {
      try {
        await this.executeOnStart(pluginId);
        lifecycle.eventUnsubscribe = this.subscribeToEvents(
          pluginId,
          lifecycle.manifest.events,
        );
        if (lifecycle.worker) {
          this.healthMonitor.startMonitoring(pluginId, lifecycle.worker);
        }
        lifecycle.state = 'RUNNING';
        lifecycle.started_at = new Date();
        lifecycle.error = undefined;
        return;
      } catch (error) {
        lifecycle.state = 'START_ERROR';
        lifecycle.error = error instanceof Error ? error.message : String(error);
        throw error;
      }
    }

    if (fromState === 'RUNNING' && toState === 'STOPPING') {
      lifecycle.state = 'STOPPING';
      lifecycle.eventUnsubscribe?.();
      lifecycle.eventUnsubscribe = undefined;
      this.healthMonitor.stopMonitoring(pluginId);
      return;
    }

    if (fromState === 'STOPPING' && toState === 'STOPPED') {
      await this.stopPlugin(pluginId, STOP_TIMEOUT_MS);
      return;
    }

    if (fromState === 'STOPPED' && toState === 'DESTROYED') {
      await this.executeOnDestroy(pluginId);
      lifecycle.state = 'DESTROYED';
      lifecycle.stopped_at = new Date();
      lifecycle.bridgeUnsubscribe?.();
      lifecycle.bridgeUnsubscribe = undefined;
      lifecycle.eventUnsubscribe = undefined;
      lifecycle.worker = undefined;
      lifecycle.port = undefined;
      lifecycle.context = undefined;
      return;
    }

    lifecycle.state = toState;
  }

  /**
   * 逻辑块：初始化阶段桥接绑定与插件握手。
   * - 目的：创建 Isolate、绑定 Context 代理并触发 onInit。
   * - 原因：插件只允许通过桥接层调用宿主能力，禁止直接越界访问。
   * - 失败路径：任一步骤失败即销毁 Isolate 并回滚监听器。
   */
  public async executeOnInit(
    pluginId: string,
    context: PluginContext,
  ): Promise<void> {
    const lifecycle = this.manage(pluginId);
    const isolateId = lifecycle.isolateId;
    const isolate = await this.isolateManager.createIsolate(
      isolateId,
      lifecycle.manifest,
      lifecycle.entryPath,
    );

    lifecycle.worker = isolate.worker;
    lifecycle.port = isolate.port;
    lifecycle.context = context;

    lifecycle.bridgeUnsubscribe?.();
    lifecycle.bridgeUnsubscribe = this.bindInvokeBridge(pluginId, isolate.worker);

    await this.invokeHook(pluginId, 'onInit', {
      hasContext: context !== undefined,
    });
  }

  public async executeOnStart(pluginId: string): Promise<void> {
    await this.invokeHook(pluginId, 'onStart', {});
  }

  public async executeOnStop(pluginId: string): Promise<void> {
    await this.invokeHook(pluginId, 'onStop', {});
  }

  public async executeOnDestroy(pluginId: string): Promise<void> {
    const lifecycle = this.manage(pluginId);
    if (!lifecycle.worker) {
      return;
    }

    await this.invokeHook(pluginId, 'onDestroy', {});
    await this.isolateManager.destroyIsolate(lifecycle.isolateId);
    await Promise.resolve(lifecycle.worker.terminate());
  }

  public subscribeToEvents(pluginId: string, events: string[]): () => void {
    const subscriptions: Subscription[] = [];
    const traceContext = this.createPluginTraceContext(pluginId);

    for (const subject of events) {
      void this.natsSubscribe(traceContext, subject, async (msg: Msg) => {
        this.eventBridge.publish(subject, decodeNatsPayload(msg));
      }).then((sub) => {
        subscriptions.push(sub);
      });
    }

    return () => {
      for (const sub of subscriptions) {
        sub.unsubscribe();
      }
    };
  }

  /**
   * 逻辑块：STOP 阶段优雅关闭。
   * - 目的：先发 SIGTERM 请求插件自清理，再给固定超时窗口。
   * - 原因：保障插件有机会执行 onStop 释放资源，超时时防止僵尸隔离体占用内存。
   * - 降级：超时后立即执行 worker.terminate() 强制回收并写入 STOPPED。
   */
  public async stopPlugin(pluginId: string, timeoutMs = STOP_TIMEOUT_MS): Promise<void> {
    const lifecycle = this.manage(pluginId);
    if (lifecycle.state !== 'RUNNING' && lifecycle.state !== 'STOPPING') {
      throw new Error(`Cannot stop plugin ${pluginId} in state ${lifecycle.state}`);
    }

    lifecycle.state = 'STOPPING';
    lifecycle.eventUnsubscribe?.();
    lifecycle.eventUnsubscribe = undefined;
    this.healthMonitor.stopMonitoring(pluginId);

    const worker = lifecycle.worker;
    if (!worker) {
      lifecycle.state = 'STOPPED';
      lifecycle.stopped_at = new Date();
      return;
    }

    await this.messageBridge.sendMessage(
      worker,
      createLifecycleMessage({
        pluginId,
        type: PluginMessageType.TERMINATE,
        payload: { signal: 'SIGTERM' },
      }),
    );

    let timedOut = false;
    const timeoutPromise = new Promise<never>((_, reject) => {
      const timer = setTimeout(() => {
        timedOut = true;
        reject(new Error(`Plugin ${pluginId} stop timeout after ${timeoutMs}ms`));
      }, timeoutMs);

      void timer;
    });

    try {
      await Promise.race([this.executeOnStop(pluginId), timeoutPromise]);
    } catch (error) {
      lifecycle.error = error instanceof Error ? error.message : String(error);

      if (timedOut) {
        await Promise.resolve(worker.terminate());
      }
    } finally {
      await this.isolateManager.destroyIsolate(lifecycle.isolateId);
      await Promise.resolve(worker.terminate());
      lifecycle.state = 'STOPPED';
      lifecycle.stopped_at = new Date();
      lifecycle.worker = undefined;
      lifecycle.port = undefined;
      lifecycle.oldWorker = undefined;
      lifecycle.newWorker = undefined;
    }
  }

  public getRegistry(): ReadonlyMap<string, PluginLifecycle> {
    return this.registry;
  }

  private async invokeHook(
    pluginId: string,
    method: string,
    params: unknown,
    timeoutMs = STOP_TIMEOUT_MS,
    workerOverride?: Worker,
  ): Promise<unknown> {
    const worker = workerOverride ?? this.manage(pluginId).worker;
    if (!worker) {
      throw new Error(`Plugin ${pluginId} has no running isolate`);
    }

    const request: PluginInvokeRequest = {
      method,
      params,
    };

    const response = await this.messageBridge.sendMessageAndWait(
      worker,
      createLifecycleMessage({
        pluginId,
        type: PluginMessageType.INVOKE,
        payload: request,
      }),
      timeoutMs,
    );

    if (!isInvokeResponse(response.payload)) {
      return undefined;
    }

    if (!response.payload.success) {
      throw new Error(
        response.payload.error?.message ??
          `Plugin ${pluginId} hook ${method} failed without reason`,
      );
    }

    return response.payload.data;
  }

  /**
   * 逻辑块：绑定插件 Worker 的 Invoke 桥接。
   * - 目的：复用统一的 request/response 转发逻辑，避免 reload 与 init 分叉实现。
   * - 原因：插件上下文调用必须由宿主桥接层统一拦截并返回协议化错误。
   * - 失败路径：上下文处理异常时返回 PLUGIN_CONTEXT_BRIDGE_ERROR，不中断主循环。
   */
  private bindInvokeBridge(pluginId: string, worker: Worker): () => void {
    return this.messageBridge.onMessage(worker, (message: PluginMessage) => {
      if (message.type !== PluginMessageType.INVOKE) {
        return;
      }

      if (!isInvokeRequest(message.payload)) {
        return;
      }

      void this.contextBridge
        .handleInvokeRequest(pluginId, message.payload)
        .then((result) =>
          this.messageBridge.sendMessage(
            worker,
            createLifecycleMessage({
              id: message.id,
              pluginId,
              type: PluginMessageType.INVOKE_RESULT,
              payload: result,
            }),
          ),
        )
        .catch((error) =>
          this.messageBridge.sendMessage(
            worker,
            createLifecycleMessage({
              id: message.id,
              pluginId,
              type: PluginMessageType.INVOKE_RESULT,
              payload: {
                success: false,
                error: {
                  code: 'PLUGIN_CONTEXT_BRIDGE_ERROR',
                  message: error instanceof Error ? error.message : String(error),
                },
              } satisfies PluginInvokeResponse,
            }),
          ),
        );
    });
  }

  /**
   * 逻辑块：双 Worker 热重载编排。
   * - 目的：先拉起新隔离体验证 onInit/onStart，再切流并回收旧隔离体。
   * - 原因：保证新版本失败时旧版本持续服务，实现最小中断升级。
   * - 失败路径：任何初始化/启动/超时/版本落库失败都销毁新隔离体并回滚到旧 Worker。
   */
  public async reloadPlugin(
    pluginId: string,
    startupTimeoutMs: number,
    onVersionPersist: () => Promise<number>,
  ): Promise<{ success: boolean; error?: string; version?: number }> {
    const lifecycle = this.manage(pluginId);
    if (lifecycle.state !== 'RUNNING' || !lifecycle.worker) {
      return {
        success: false,
        error: `Plugin ${pluginId} is not running`,
      };
    }

    const previousState = lifecycle.state;
    const previousWorker = lifecycle.worker;
    const previousPort = lifecycle.port;
    const previousBridgeUnsubscribe = lifecycle.bridgeUnsubscribe;
    const previousEventUnsubscribe = lifecycle.eventUnsubscribe;
    const previousIsolateId = lifecycle.isolateId;
    const previousStartedAt = lifecycle.started_at;

    const nextIsolateId = `${pluginId}#reload#${Date.now()}`;
    lifecycle.state = 'RELOADING';
    lifecycle.oldWorker = previousWorker;
    lifecycle.newWorker = undefined;
    lifecycle.error = undefined;

    try {
      const nextContext = this.contextBridge.createContext(pluginId, lifecycle.manifest);
      const nextIsolate = await this.isolateManager.createIsolate(
        nextIsolateId,
        lifecycle.manifest,
        lifecycle.entryPath,
      );

      lifecycle.newWorker = nextIsolate.worker;
      const nextBridgeUnsubscribe = this.bindInvokeBridge(pluginId, nextIsolate.worker);

      try {
        await this.invokeHook(
          pluginId,
          'onInit',
          { hasContext: nextContext !== undefined, reload: true },
          startupTimeoutMs,
          nextIsolate.worker,
        );
        await this.invokeHook(
          pluginId,
          'onStart',
          { reload: true },
          startupTimeoutMs,
          nextIsolate.worker,
        );
      } catch (error) {
        nextBridgeUnsubscribe();
        await this.isolateManager.destroyIsolate(nextIsolateId);
        lifecycle.newWorker = undefined;
        lifecycle.oldWorker = undefined;
        lifecycle.state = previousState;
        lifecycle.error = error instanceof Error ? error.message : String(error);
        return { success: false, error: lifecycle.error };
      }

      const nextConfigVersion = await onVersionPersist();

      previousEventUnsubscribe?.();
      previousBridgeUnsubscribe?.();

      this.healthMonitor.startMonitoring(pluginId, nextIsolate.worker);
      lifecycle.eventUnsubscribe = this.subscribeToEvents(pluginId, lifecycle.manifest.events);

      lifecycle.worker = nextIsolate.worker;
      lifecycle.port = nextIsolate.port;
      lifecycle.context = nextContext;
      lifecycle.bridgeUnsubscribe = nextBridgeUnsubscribe;
      lifecycle.started_at = new Date();
      lifecycle.isolateId = nextIsolateId;
      lifecycle.configVersion = nextConfigVersion;

      await this.executeOnStop(pluginId);
      await this.isolateManager.destroyIsolate(previousIsolateId);
      await Promise.resolve(previousWorker.terminate());

      lifecycle.oldWorker = undefined;
      lifecycle.newWorker = undefined;
      lifecycle.state = 'RUNNING';
      lifecycle.error = undefined;

      return { success: true, version: nextConfigVersion };
    } catch (error) {
      lifecycle.worker = previousWorker;
      lifecycle.port = previousPort;
      lifecycle.bridgeUnsubscribe = previousBridgeUnsubscribe;
      lifecycle.eventUnsubscribe = previousEventUnsubscribe;
      lifecycle.isolateId = previousIsolateId;
      lifecycle.started_at = previousStartedAt;
      lifecycle.oldWorker = undefined;
      lifecycle.newWorker = undefined;
      lifecycle.state = previousState;
      lifecycle.error = error instanceof Error ? error.message : String(error);
      return { success: false, error: lifecycle.error };
    }
  }

  public async invoke(
    pluginId: string,
    method: string,
    params: unknown,
    timeoutMs = RELOAD_STARTUP_TIMEOUT_MS,
  ): Promise<InvokePluginMethodResult> {
    let lifecycle: PluginLifecycle;
    try {
      lifecycle = this.manage(pluginId);
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }

    if (lifecycle.state !== 'RUNNING') {
      return {
        success: false,
        error: `Plugin ${pluginId} is not running`,
      };
    }

    try {
      const data = await this.invokeHook(pluginId, method, params, timeoutMs);
      return {
        success: true,
        data,
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }
}

export const createLifecycleManager = (
  options: PluginLifecycleManagerOptions = {},
): PluginLifecycleManager => new PluginLifecycleManager(options);

export type LoadPluginResult =
  | { success: true; manifest: PluginManifest }
  | { success: false; errors: string[] };

const pluginRegistry = new Map<string, PluginLifecycle>();
const lifecycleManager = createLifecycleManager({
  registry: pluginRegistry,
});
let pluginDb: Db | null = null;

const bumpPluginConfigVersion = async (pluginId: string): Promise<number> => {
  if (!pluginDb) {
    throw new Error('Plugin database is not initialized');
  }

  const updated = await pluginDb.collection<PluginDocument>(PLUGINS_COLLECTION).findOneAndUpdate(
    { plugin_id: pluginId },
    {
      $inc: { 'config.v': 1 },
      $set: { updated_at: new Date() },
    },
    {
      returnDocument: 'after',
    },
  );

  const version = updated?.config?.v;
  if (typeof version !== 'number') {
    throw new Error(`Failed to increment config version for plugin ${pluginId}`);
  }

  return version;
};

export function getPluginRegistry(): ReadonlyMap<string, PluginLifecycle> {
  return lifecycleManager.getRegistry();
}

export function getPluginInstance(pluginId: string): PluginLifecycle | undefined {
  return pluginRegistry.get(pluginId);
}

export function getPluginHealthStatus(pluginId: string): HealthStatus {
  return lifecycleManager.getHealth(pluginId);
}

export function isPluginResponsive(pluginId: string): boolean {
  return lifecycleManager.isResponsive(pluginId);
}

export async function invokePluginMethod(
  pluginId: string,
  method: string,
  params: unknown,
  timeoutMs?: number,
): Promise<InvokePluginMethodResult> {
  return lifecycleManager.invoke(pluginId, method, params, timeoutMs);
}

export function parseManifest(manifestJson: string): LoadPluginResult {
  let manifest: unknown;
  try {
    manifest = JSON.parse(manifestJson);
  } catch {
    return { success: false, errors: ['Invalid JSON format'] };
  }

  const validation = validateManifest(manifest);
  if (!validation.valid) {
    return {
      success: false,
      errors: validation.errors.map((error) => `${error.field}: ${error.message}`),
    };
  }

  return { success: true, manifest: manifest as PluginManifest };
}

const FILE_PATH_EXTENSIONS = new Set(['.ts', '.mts', '.cts', '.js', '.mjs', '.cjs']);

const isLikelyFilePath = (value: string): boolean => {
  const ext = extname(value).toLowerCase();
  if (FILE_PATH_EXTENSIONS.has(ext)) {
    return true;
  }

  if (!existsSync(value)) {
    return false;
  }

  try {
    return statSync(value).isFile();
  } catch {
    return false;
  }
};

/**
 * 逻辑块：插件入口路径统一解析。
 * - 目的：兼容“直接传入口文件”和“传插件目录 + manifest.entry”两种调用方式。
 * - 原因：现有测试传文件路径，生产安装通常传目录；统一收敛可避免调用方分叉。
 * - 失败路径：路径穿越、绝对 entry、入口文件不存在都会返回错误并阻断加载。
 */
export const resolvePluginEntryPath = (
  pluginPath: string,
  manifest: PluginManifest,
): { success: true; entryPath: string } | { success: false; error: string } => {
  if (!pluginPath || pluginPath.trim().length === 0) {
    return { success: false, error: 'plugin path is empty' };
  }

  const normalizedPluginPath = resolve(pluginPath);
  if (isLikelyFilePath(normalizedPluginPath)) {
    if (!existsSync(normalizedPluginPath)) {
      return {
        success: false,
        error: `plugin entry does not exist: ${normalizedPluginPath}`,
      };
    }
    return { success: true, entryPath: normalizedPluginPath };
  }

  if (isAbsolute(manifest.entry)) {
    return {
      success: false,
      error: `manifest entry must be relative: ${manifest.entry}`,
    };
  }

  const normalizedEntry = manifest.entry.replace(/^\.(?:[\\/])+/u, '');
  const resolvedEntryPath = resolve(normalizedPluginPath, normalizedEntry);
  const expectedPrefix = `${normalizedPluginPath}${sep}`;
  if (
    resolvedEntryPath !== normalizedPluginPath
    && !resolvedEntryPath.startsWith(expectedPrefix)
  ) {
    return {
      success: false,
      error: `manifest entry escapes plugin root: ${manifest.entry}`,
    };
  }

  if (!existsSync(resolvedEntryPath)) {
    return {
      success: false,
      error: `plugin entry does not exist: ${resolvedEntryPath}`,
    };
  }

  return { success: true, entryPath: resolvedEntryPath };
};

export async function loadPlugin(
  db: Db,
  manifest: PluginManifest,
  pluginPath: string,
): Promise<{ success: boolean; error?: string }> {
  pluginDb = db;
  if (pluginRegistry.has(manifest.id)) {
    return { success: false, error: `Plugin ${manifest.id} is already loaded` };
  }

  const resolvedEntry = resolvePluginEntryPath(pluginPath, manifest);
  if (!resolvedEntry.success) {
    return { success: false, error: resolvedEntry.error };
  }

  const doc: PluginDocument = {
    plugin_id: manifest.id,
    name: manifest.name,
    version: manifest.version,
    entry: manifest.entry,
    ui: {
      entry: manifest.ui.entry,
      mode: manifest.ui.mode,
      icon: manifest.ui.icon,
      sdui_version: manifest.sdui_version,
    },
    permissions: manifest.permissions,
    events: manifest.events,
    exports: manifest.exports,
    config: {
      encrypted: Buffer.alloc(0),
      schema: {},
      v: 1,
    },
    status: 'ACTIVE',
    installed_at: new Date(),
    updated_at: new Date(),
  };

  await db
    .collection(PLUGINS_COLLECTION)
    .updateOne({ plugin_id: manifest.id }, { $set: doc }, { upsert: true });

  pluginRegistry.set(manifest.id, {
    manifest,
    state: 'LOADED',
    config: {},
    entryPath: resolvedEntry.entryPath,
    isolateId: manifest.id,
    configVersion: 1,
  });

  return { success: true };
}

export async function unloadPlugin(
  db: Db,
  pluginId: string,
): Promise<{ success: boolean; error?: string }> {
  const lifecycle = pluginRegistry.get(pluginId);
  if (!lifecycle) {
    return { success: false, error: `Plugin ${pluginId} not found` };
  }

  try {
    if (lifecycle.state === 'RUNNING') {
      await lifecycleManager.transition(pluginId, 'STOPPING');
      await lifecycleManager.transition(pluginId, 'STOPPED');
    }

    if (lifecycle.state === 'STOPPED') {
      await lifecycleManager.transition(pluginId, 'DESTROYED');
    }
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }

  pluginRegistry.delete(pluginId);
  await db.collection(PLUGINS_COLLECTION).updateOne(
    { plugin_id: pluginId },
    { $set: { status: 'DISABLED' as const, updated_at: new Date() } },
  );

  return { success: true };
}

export async function initPlugin(
  pluginId: string,
): Promise<{ success: boolean; error?: string }> {
  try {
    await lifecycleManager.transition(pluginId, 'INITIALIZING');
    return { success: true };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

export async function startPlugin(
  pluginId: string,
): Promise<{ success: boolean; error?: string }> {
  const lifecycle = pluginRegistry.get(pluginId);
  if (!lifecycle) {
    return { success: false, error: `Plugin ${pluginId} not found` };
  }

  try {
    if (lifecycle.state === 'LOADED') {
      await lifecycleManager.transition(pluginId, 'INITIALIZING');
    }

    if (lifecycleManager.getState(pluginId) === 'STARTING') {
      await lifecycleManager.transition(pluginId, 'RUNNING');
    }

    if (lifecycleManager.getState(pluginId) !== 'RUNNING') {
      return {
        success: false,
        error: `Cannot start plugin in state ${lifecycleManager.getState(pluginId)}`,
      };
    }

    return { success: true };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

export async function stopPlugin(
  pluginId: string,
  timeoutMs = STOP_TIMEOUT_MS,
): Promise<{ success: boolean; error?: string }> {
  const lifecycle = pluginRegistry.get(pluginId);
  if (!lifecycle) {
    return { success: false, error: `Plugin ${pluginId} not found` };
  }

  if (lifecycle.state !== 'RUNNING') {
    return {
      success: false,
      error: `Cannot stop plugin in state ${lifecycle.state}`,
    };
  }

  try {
    await lifecycleManager.stopPlugin(pluginId, timeoutMs);
    return { success: true };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

export async function reloadPlugin(
  pluginId: string,
): Promise<{ success: boolean; error?: string }> {
  const lifecycle = pluginRegistry.get(pluginId);
  if (!lifecycle) {
    return { success: false, error: `Plugin ${pluginId} not found` };
  }

  const result = await lifecycleManager.reloadPlugin(
    pluginId,
    RELOAD_STARTUP_TIMEOUT_MS,
    async () => bumpPluginConfigVersion(pluginId),
  );

  if (!result.success) {
    return {
      success: false,
      error: result.error,
    };
  }

  if (typeof result.version === 'number') {
    lifecycle.configVersion = result.version;
  }

  return { success: true };
}

export async function destroyPlugin(
  pluginId: string,
): Promise<{ success: boolean; error?: string }> {
  const lifecycle = pluginRegistry.get(pluginId);
  if (!lifecycle) {
    return { success: false, error: `Plugin ${pluginId} not found` };
  }

  try {
    if (lifecycle.state === 'RUNNING') {
      await lifecycleManager.transition(pluginId, 'STOPPING');
      await lifecycleManager.transition(pluginId, 'STOPPED');
    }

    if (lifecycleManager.getState(pluginId) === 'STOPPED') {
      await lifecycleManager.transition(pluginId, 'DESTROYED');
    }

    pluginRegistry.delete(pluginId);
    return { success: true };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

export function computeLoadOrder(): PluginLoadOrder {
  const manifests = new Map<string, PluginManifest>();
  for (const [id, lifecycle] of pluginRegistry) {
    manifests.set(id, lifecycle.manifest);
  }
  return topologicalSort(manifests);
}

export function validateAllPlugins(): ManifestValidationResult {
  const manifests = new Map<string, PluginManifest>();
  for (const [id, lifecycle] of pluginRegistry) {
    manifests.set(id, lifecycle.manifest);
  }
  return validatePluginTopology(manifests);
}

export async function loadAllPluginsInOrder(
  db: Db,
  manifests: PluginManifest[],
  pluginBasePath: string,
): Promise<{ loaded: string[]; failed: Array<{ id: string; error: string }> }> {
  const tempRegistry = new Map<string, PluginManifest>();
  for (const manifest of manifests) {
    tempRegistry.set(manifest.id, manifest);
  }

  const topologyValidation = validatePluginTopology(tempRegistry);
  if (!topologyValidation.valid) {
    return {
      loaded: [],
      failed: topologyValidation.errors.map((error) => ({
        id: error.field ?? 'unknown',
        error: error.message,
      })),
    };
  }

  const loadOrder = topologicalSort(tempRegistry);
  const loaded: string[] = [];
  const failed: Array<{ id: string; error: string }> = [];

  for (const pluginId of loadOrder.order) {
    const manifest = tempRegistry.get(pluginId);
    if (!manifest) {
      continue;
    }

    const result = await loadPlugin(db, manifest, join(pluginBasePath, pluginId));
    if (result.success) {
      loaded.push(pluginId);
    } else {
      failed.push({ id: pluginId, error: result.error ?? 'Unknown error' });
    }
  }

  return { loaded, failed };
}

import type { PluginManifest } from '@insnh-gd/meristem-shared';
import type {
  MServiceRequest,
  PluginMessage,
  PluginInvokeRequest,
  PluginInvokeResponse,
} from '@insnh-gd/meristem-shared';
import {
  getRequiredPermissionForSubject,
  PermissionError,
  requirePermission,
  setPluginPermissions,
} from './plugin-permission';
import { MServiceRouter } from './m-service-router';
import { decryptConfig, encryptConfig } from './plugin-config-crypto';
import { QuotaManager, enforceQuota } from './storage-quota';

export const DEFAULT_METHOD_TIMEOUT_MS = 5000;
export const DEFAULT_CONFIG_QUOTA_BYTES = 100 * 1024 * 1024;

const DEFAULT_CONFIG_SECRET = 'meristem-plugin-config-secret';

type PendingRequestEntry = {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

type PluginMessageHandler = (message: PluginMessage) => void;
type PluginNodeRecord = Record<string, unknown>;
type PluginEventHandler = (data: unknown) => void;

type PluginContextState = {
  manifest: PluginManifest;
  config: Record<string, unknown>;
  context: PluginContext;
};

export type PluginContext = Readonly<{
  getNodes: () => Promise<PluginNodeRecord[]>;
  publishEvent: (subject: string, data: unknown) => Promise<void>;
  getConfig: () => Promise<Record<string, unknown>>;
  setConfig: (config: Record<string, unknown>) => Promise<void>;
  callService: (service: string, method: string, params: unknown) => Promise<unknown>;
}>;

export type PluginContextBridgeOptions = Readonly<{
  mServiceRouter?: MServiceRouter;
  listNodes?: (pluginId: string) => Promise<PluginNodeRecord[]>;
  filterNodes?: (input: {
    pluginId: string;
    manifest: PluginManifest;
    nodes: PluginNodeRecord[];
  }) => PluginNodeRecord[];
  publishEvent?: (input: {
    pluginId: string;
    subject: string;
    data: unknown;
  }) => Promise<void>;
  persistConfig?: (input: {
    pluginId: string;
    config: Record<string, unknown>;
  }) => Promise<void>;
  getEncryptedConfig?: (pluginId: string) => Promise<Buffer | null | undefined>;
  storeEncryptedConfig?: (pluginId: string, encrypted: Buffer) => Promise<void>;
  configSecret?: string;
  quotaManager?: QuotaManager;
  callService?: (input: {
    pluginId: string;
    service: string;
    method: string;
    params: unknown;
    timeoutMs: number;
  }) => Promise<unknown>;
}>;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const toError = (value: unknown): Error => {
  if (value instanceof Error) {
    return value;
  }

  return new Error(String(value));
};

const cloneConfig = (config: Record<string, unknown>): Record<string, unknown> => ({
  ...config,
});

const normalizeQuotaBytes = (quotaBytes: number): number => {
  if (!Number.isFinite(quotaBytes) || quotaBytes <= 0) {
    return DEFAULT_CONFIG_QUOTA_BYTES;
  }

  return quotaBytes;
};

const resolveConfigSecret = (secret?: string): string => {
  if (typeof secret === 'string' && secret.length > 0) {
    return secret;
  }

  const envSecret = process.env.MERISTEM_PLUGIN_CONFIG_SECRET;
  if (typeof envSecret === 'string' && envSecret.length > 0) {
    return envSecret;
  }

  return DEFAULT_CONFIG_SECRET;
};

const normalizeTimeoutMs = (timeoutMs: number | undefined): number => {
  if (typeof timeoutMs !== 'number' || !Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return DEFAULT_METHOD_TIMEOUT_MS;
  }

  return timeoutMs;
};

const isPluginMessage = (value: unknown): value is PluginMessage => {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.id === 'string' &&
    typeof value.pluginId === 'string' &&
    typeof value.type === 'string' &&
    typeof value.timestamp === 'number'
  );
};

const withTimeout = async <T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string
): Promise<T> => {
  const effectiveTimeoutMs = normalizeTimeoutMs(timeoutMs);
  let timer: ReturnType<typeof setTimeout> | undefined;

  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`${label} timed out after ${effectiveTimeoutMs}ms`));
        }, effectiveTimeoutMs);
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
};

const parsePublishEventParams = (params: unknown): { subject: string; data: unknown } => {
  if (!isRecord(params) || typeof params.subject !== 'string') {
    throw new Error('publishEvent requires { subject: string; data: unknown }');
  }

  return {
    subject: params.subject,
    data: params.data,
  };
};

const parseCallServiceParams = (params: unknown): {
  service: string;
  method: string;
  params: unknown;
  timeoutMs?: number;
} => {
  if (!isRecord(params)) {
    throw new Error(
      'callService requires { service: string; method: string; params: unknown }'
    );
  }

  const service = params.service;
  const method = params.method;
  const timeoutCandidate =
    typeof params.timeoutMs === 'number'
      ? params.timeoutMs
      : typeof params.timeout === 'number'
        ? params.timeout
        : undefined;

  if (typeof service !== 'string' || service.length === 0) {
    throw new Error('callService requires non-empty service');
  }

  if (typeof method !== 'string' || method.length === 0) {
    throw new Error('callService requires non-empty method');
  }

  return {
    service,
    method,
    params: params.params,
    timeoutMs: timeoutCandidate,
  };
};

export class RequestRegistry {
  public readonly pending = new Map<string, PendingRequestEntry>();

  public register(requestId: string, timeoutMs: number): Promise<unknown> {
    if (this.pending.has(requestId)) {
      return Promise.reject(new Error(`Request ${requestId} is already pending`));
    }

    const effectiveTimeoutMs = normalizeTimeoutMs(timeoutMs);

    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.reject(
          requestId,
          new Error(`Request ${requestId} timed out after ${effectiveTimeoutMs}ms`)
        );
      }, effectiveTimeoutMs);

      this.pending.set(requestId, {
        resolve,
        reject,
        timer,
      });
    });
  }

  public resolve(requestId: string, result: unknown): void {
    const entry = this.pending.get(requestId);
    if (!entry) {
      return;
    }

    entry.resolve(result);
    this.cleanup(requestId);
  }

  public reject(requestId: string, error: Error): void {
    const entry = this.pending.get(requestId);
    if (!entry) {
      return;
    }

    entry.reject(error);
    this.cleanup(requestId);
  }

  public cleanup(requestId: string): void {
    const entry = this.pending.get(requestId);
    if (!entry) {
      return;
    }

    clearTimeout(entry.timer);
    this.pending.delete(requestId);
  }
}

export class MessageBridge {
  private readonly handlersByWorker = new WeakMap<Worker, Set<PluginMessageHandler>>();
  private readonly dispatchersByWorker = new WeakMap<Worker, EventListener>();

  public constructor(private readonly requestRegistry = new RequestRegistry()) {}

  public async sendMessage(worker: Worker, message: PluginMessage): Promise<void> {
    this.ensureWorkerDispatcher(worker);
    worker.postMessage(message);
  }

  public async sendMessageAndWait(
    worker: Worker,
    message: PluginMessage,
    timeoutMs = DEFAULT_METHOD_TIMEOUT_MS
  ): Promise<PluginMessage> {
    this.ensureWorkerDispatcher(worker);
    const waitPromise = this.requestRegistry.register(message.id, timeoutMs);

    try {
      await this.sendMessage(worker, message);
    } catch (error) {
      this.requestRegistry.reject(message.id, toError(error));
      throw error;
    }

    const response = await waitPromise;
    if (!isPluginMessage(response)) {
      throw new Error(`Invalid plugin message response for request ${message.id}`);
    }

    return response;
  }

  public onMessage(worker: Worker, handler: (msg: PluginMessage) => void): () => void {
    const handlers = this.ensureWorkerDispatcher(worker);
    handlers.add(handler);

    return () => {
      handlers.delete(handler);
    };
  }

  public generateMessageId(): string {
    if (typeof crypto.randomUUID === 'function') {
      return crypto.randomUUID();
    }

    return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  /**
   * 逻辑块：统一 Worker 消息分发入口。
   * - 目的：同一条消息同时驱动 request-reply 超时收敛与外部订阅回调。
   * - 原因：避免为每次请求重复注册监听器，减少资源泄漏点。
   * - 失败路径：非协议消息直接丢弃，不影响已注册请求与事件处理。
   */
  private ensureWorkerDispatcher(worker: Worker): Set<PluginMessageHandler> {
    const existingHandlers = this.handlersByWorker.get(worker);
    if (existingHandlers) {
      return existingHandlers;
    }

    const handlers = new Set<PluginMessageHandler>();
    const dispatcher: EventListener = (event: Event) => {
      const messageEvent = event as MessageEvent<unknown>;
      if (!isPluginMessage(messageEvent.data)) {
        return;
      }

      this.requestRegistry.resolve(messageEvent.data.id, messageEvent.data);

      for (const handler of handlers) {
        handler(messageEvent.data);
      }
    };

    this.handlersByWorker.set(worker, handlers);
    this.dispatchersByWorker.set(worker, dispatcher);
    worker.addEventListener('message', dispatcher);

    return handlers;
  }
}

export class PluginContextBridge {
  private readonly contexts = new Map<string, PluginContextState>();
  private readonly listNodes: (pluginId: string) => Promise<PluginNodeRecord[]>;
  private readonly filterNodes: (input: {
    pluginId: string;
    manifest: PluginManifest;
    nodes: PluginNodeRecord[];
  }) => PluginNodeRecord[];
  private readonly publish: (input: {
    pluginId: string;
    subject: string;
    data: unknown;
  }) => Promise<void>;
  private readonly persistConfig: (input: {
    pluginId: string;
    config: Record<string, unknown>;
  }) => Promise<void>;
  private readonly getEncryptedConfigByPluginId: (
    pluginId: string
  ) => Promise<Buffer | null | undefined>;
  private readonly storeEncryptedConfigByPluginId: (
    pluginId: string,
    encrypted: Buffer
  ) => Promise<void>;
  private readonly callServiceProxy: (input: {
    pluginId: string;
    service: string;
    method: string;
    params: unknown;
    timeoutMs: number;
  }) => Promise<unknown>;
  private readonly secret: string;
  private readonly quota: number;
  private readonly quotaManager: QuotaManager;

  public constructor(
    options: PluginContextBridgeOptions = {},
    mServiceRouter?: MServiceRouter,
    quota = DEFAULT_CONFIG_QUOTA_BYTES
  ) {
    this.listNodes = options.listNodes ?? (async () => []);
    this.filterNodes = options.filterNodes ?? (input => input.nodes);
    this.publish = options.publishEvent ?? (async () => undefined);
    this.persistConfig = options.persistConfig ?? (async () => undefined);
    this.getEncryptedConfigByPluginId = options.getEncryptedConfig ?? (async () => null);
    this.storeEncryptedConfigByPluginId = options.storeEncryptedConfig ?? (async () => undefined);
    this.secret = resolveConfigSecret(options.configSecret);
    this.quota = normalizeQuotaBytes(quota);
    this.quotaManager = options.quotaManager ?? new QuotaManager(this.quota);
    const resolvedMServiceRouter = mServiceRouter ?? options.mServiceRouter;

    this.callServiceProxy =
      options.callService ??
      (resolvedMServiceRouter
        ? async input => this.routeMService(resolvedMServiceRouter, input)
        : async input => ({
            success: false,
            error: {
              code: 'SERVICE_UNAVAILABLE',
              message: `M-Service router is not bound for ${input.service}.${input.method}`,
            },
          }));
  }

  public createContext(pluginId: string, manifest: PluginManifest): PluginContext {
    const existing = this.contexts.get(pluginId);
    setPluginPermissions(pluginId, manifest.permissions as readonly string[]);

    if (existing) {
      existing.manifest = manifest;
      return existing.context;
    }

    const context: PluginContext = {
      getNodes: async () => this.getNodes(pluginId),
      publishEvent: async (subject: string, data: unknown) =>
        this.publishEvent(pluginId, subject, data),
      getConfig: async () => this.getConfig(pluginId),
      setConfig: async (config: Record<string, unknown>) =>
        this.setConfig(pluginId, config),
      callService: async (service: string, method: string, params: unknown) =>
        this.callService(pluginId, service, method, params, DEFAULT_METHOD_TIMEOUT_MS),
    };

    this.contexts.set(pluginId, {
      manifest,
      config: {},
      context,
    });

    return context;
  }

  /**
   * 逻辑块：统一解析来自插件侧的 Context 调用。
   * - 目的：保证插件只能访问受控方法集合，避免透传任意宿主能力。
   * - 原因：插件运行在隔离环境，必须在桥接层完成显式方法白名单。
   * - 降级：非法方法或参数立即抛错，由上层转译为 invoke error 响应。
   */
  public async handleContextRequest(
    pluginId: string,
    request: { method: string; params: unknown }
  ): Promise<unknown> {
    const context = this.getContext(pluginId);

    switch (request.method) {
      case 'getNodes':
        return context.getNodes();
      case 'publishEvent': {
        const params = parsePublishEventParams(request.params);
        return context.publishEvent(params.subject, params.data);
      }
      case 'getConfig':
        return context.getConfig();
      case 'setConfig': {
        if (!isRecord(request.params)) {
          throw new Error('setConfig requires Record<string, unknown>');
        }

        return context.setConfig(request.params);
      }
      case 'callService': {
        const params = parseCallServiceParams(request.params);
        const timeoutMs = this.resolveTimeoutMs(request, params.timeoutMs);
        return this.callService(
          pluginId,
          params.service,
          params.method,
          params.params,
          timeoutMs
        );
      }
      default:
        throw new Error(`Unsupported plugin context method: ${request.method}`);
    }
  }

  public async handleInvokeRequest(
    pluginId: string,
    request: PluginInvokeRequest
  ): Promise<PluginInvokeResponse> {
    try {
      const data = await this.handleContextRequest(pluginId, request);
      return {
        success: true,
        data,
      };
    } catch (error) {
      if (error instanceof PermissionError) {
        return {
          success: false,
          error: {
            code: 'PERMISSION_DENIED',
            message: error.message,
          },
        };
      }

      const normalizedError = toError(error);
      return {
        success: false,
        error: {
          code: 'PLUGIN_CONTEXT_ERROR',
          message: normalizedError.message,
        },
      };
    }
  }

  private getContext(pluginId: string): PluginContext {
    const state = this.contexts.get(pluginId);
    if (!state) {
      throw new Error(`Plugin context not found: ${pluginId}`);
    }

    return state.context;
  }

  private getState(pluginId: string): PluginContextState {
    const state = this.contexts.get(pluginId);
    if (!state) {
      throw new Error(`Plugin context not found: ${pluginId}`);
    }

    return state;
  }

  private async getNodes(pluginId: string): Promise<PluginNodeRecord[]> {
    requirePermission(pluginId, 'node:read');

    const state = this.getState(pluginId);

    const nodes = await this.listNodes(pluginId);
    return this.filterNodes({
      pluginId,
      manifest: state.manifest,
      nodes,
    });
  }

  /**
   * 逻辑块：发布事件前按 Subject 映射做 Layer 1 权限校验。
   * - 目的：在插件上下文调用处给出明确权限错误，避免进入总线后才失败。
   * - 原因：Layer 1 负责开发者友好提示，Layer 2 继续承担总线硬拦截。
   * - 降级：映射到权限但未授权时立即抛 PermissionError，由调用方转译错误码。
   */
  private async publishEvent(
    pluginId: string,
    subject: string,
    data: unknown
  ): Promise<void> {
    const requiredPermission = getRequiredPermissionForSubject(subject);
    if (requiredPermission) {
      requirePermission(pluginId, requiredPermission);
    }

    await this.publish({
      pluginId,
      subject,
      data,
    });
  }

  private async getConfig(pluginId: string): Promise<Record<string, unknown>> {
    /**
     * 逻辑块：优先读取持久化密文配置并解密回插件可读对象。
     * - 目的：在桥接层屏蔽密文细节，插件侧始终只接触明文结构。
     * - 原因：配置加密与解密策略属于宿主能力，不应泄漏到插件实现。
     * - 降级：未命中持久化密文时回退到当前内存快照，保证向后兼容。
     */
    const encrypted = await this.getEncryptedConfigByPluginId(pluginId);
    if (!encrypted || encrypted.length === 0) {
      return cloneConfig(this.getState(pluginId).config);
    }

    const decryptedConfig = decryptConfig(encrypted, this.secret);
    const state = this.getState(pluginId);
    state.config = cloneConfig(decryptedConfig);
    return cloneConfig(decryptedConfig);
  }

  private async setConfig(
    pluginId: string,
    config: Record<string, unknown>
  ): Promise<void> {
    if (!isRecord(config)) {
      throw new Error('setConfig requires Record<string, unknown>');
    }

    const state = this.getState(pluginId);
    const nextConfig = cloneConfig(config);

    /**
     * 逻辑块：配置写入前执行配额闸门并落库密文。
     * - 目的：在桥接层统一阻止超限写入，确保插件配置不会突破存储预算。
     * - 原因：配额与加密都属于平台治理策略，必须在宿主侧强制执行。
     * - 失败路径：超额时抛错中断写入；密文存储失败时不提交内存与持久化快照。
     */
    const configSize = JSON.stringify(nextConfig).length;
    if (!this.quotaManager.checkQuota(pluginId, configSize)) {
      enforceQuota(pluginId, configSize, this.quota);
    }
    this.quotaManager.trackUsage(pluginId, configSize);

    const encrypted = encryptConfig(nextConfig, this.secret);
    await this.storeEncryptedConfigByPluginId(pluginId, encrypted);

    state.config = nextConfig;
    await this.persistConfig({
      pluginId,
      config: cloneConfig(nextConfig),
    });
  }

  private async callService(
    pluginId: string,
    service: string,
    method: string,
    params: unknown,
    timeoutMs: number
  ): Promise<unknown> {
    requirePermission(pluginId, 'plugin:access');

    return withTimeout(
      this.callServiceProxy({
        pluginId,
        service,
        method,
        params,
        timeoutMs,
      }),
      timeoutMs,
      `M-Service ${service}.${method}`
    );
  }

  /**
   * 逻辑块：插件 callService 到 M-Service 路由器的协议转换。
   * - 目的：将 PluginContext 调用统一封装为 M-Service 请求并复用 Router 错误语义。
   * - 原因：插件互调必须保持 request/response 契约一致，不能直接透传内部实现细节。
   * - 失败路径：Router 返回失败响应时抛错，由上层 invoke 逻辑转译为插件可消费错误。
   */
  private async routeMService(
    router: MServiceRouter,
    input: {
      pluginId: string;
      service: string;
      method: string;
      params: unknown;
      timeoutMs: number;
    }
  ): Promise<unknown> {
    const request: MServiceRequest = {
      trace_id: this.generateTraceId(),
      caller: input.pluginId,
      service: input.service,
      method: input.method,
      payload: this.normalizeMServicePayload(input.params),
      timeout: input.timeoutMs,
    };

    const response = await router.route(request);
    if (response.success) {
      return response.data;
    }

    const errorCode = response.error?.code ?? 'SERVICE_UNAVAILABLE';
    const errorMessage =
      response.error?.message ??
      `M-Service ${input.service}.${input.method} returned an unknown error`;
    throw new Error(`[${errorCode}] ${errorMessage}`);
  }

  private normalizeMServicePayload(params: unknown): Record<string, unknown> {
    if (isRecord(params)) {
      return params;
    }

    if (params === undefined) {
      return {};
    }

    return { params };
  }

  private generateTraceId(): string {
    if (typeof crypto.randomUUID === 'function') {
      return crypto.randomUUID();
    }

    return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  private resolveTimeoutMs(
    request: { method: string; params: unknown } | PluginInvokeRequest,
    timeoutFromParams?: number
  ): number {
    if (typeof timeoutFromParams === 'number') {
      return normalizeTimeoutMs(timeoutFromParams);
    }

    const timeoutFromRequest =
      'timeout' in request && typeof request.timeout === 'number'
        ? request.timeout
        : undefined;

    return normalizeTimeoutMs(timeoutFromRequest);
  }
}

export { PermissionError };

export class EventBridge {
  private readonly handlersByEvent = new Map<string, Map<string, PluginEventHandler>>();
  private readonly eventsByPlugin = new Map<string, Set<string>>();

  public subscribe(
    pluginId: string,
    event: string,
    handler: (data: unknown) => void
  ): () => void {
    let eventHandlers = this.handlersByEvent.get(event);
    if (!eventHandlers) {
      eventHandlers = new Map<string, PluginEventHandler>();
      this.handlersByEvent.set(event, eventHandlers);
    }

    eventHandlers.set(pluginId, handler);

    let pluginEvents = this.eventsByPlugin.get(pluginId);
    if (!pluginEvents) {
      pluginEvents = new Set<string>();
      this.eventsByPlugin.set(pluginId, pluginEvents);
    }
    pluginEvents.add(event);

    return () => {
      this.unsubscribe(pluginId, event);
    };
  }

  public unsubscribe(pluginId: string, event: string): void {
    const eventHandlers = this.handlersByEvent.get(event);
    if (eventHandlers) {
      eventHandlers.delete(pluginId);
      if (eventHandlers.size === 0) {
        this.handlersByEvent.delete(event);
      }
    }

    const pluginEvents = this.eventsByPlugin.get(pluginId);
    if (pluginEvents) {
      pluginEvents.delete(event);
      if (pluginEvents.size === 0) {
        this.eventsByPlugin.delete(pluginId);
      }
    }
  }

  public publish(event: string, data: unknown): void {
    const eventHandlers = this.handlersByEvent.get(event);
    if (!eventHandlers) {
      return;
    }

    for (const handler of eventHandlers.values()) {
      handler(data);
    }
  }

  public getPluginSubscriptions(pluginId: string): string[] {
    const pluginEvents = this.eventsByPlugin.get(pluginId);
    if (!pluginEvents) {
      return [];
    }

    return [...pluginEvents].sort();
  }
}

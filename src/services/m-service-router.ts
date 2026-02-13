import type {
  MServiceErrorCode,
  MServiceRequest,
  MServiceResponse,
  PluginInvokeResponse,
} from '@insnh-gd/meristem-shared';
import { PluginMessageType } from '@insnh-gd/meristem-shared';
import { MessageBridge } from './plugin-bridge';
import { requirePermission } from './plugin-permission';

const DEFAULT_TIMEOUT_MS = 5000;
const BRIDGE_TIMEOUT_BUFFER_MS = 100;

const M_SERVICE_ERROR_CODES = new Set<MServiceErrorCode>([
  'SERVICE_UNAVAILABLE',
  'ACCESS_DENIED',
  'TIMEOUT',
  'METHOD_NOT_FOUND',
  'INTERNAL_ERROR',
]);

type WorkerResolver = (pluginId: string) => Worker | undefined;

export type ServiceRegistration = Readonly<{
  service: string;
  pluginId: string;
  methods: string[];
}>;

type StoredServiceRegistration = Readonly<{
  service: string;
  pluginId: string;
  methods: readonly string[];
}>;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const normalizeName = (value: string): string => value.trim();

const normalizeMethods = (methods: string[]): string[] => {
  const unique = new Set<string>();

  for (const method of methods) {
    const normalized = normalizeName(method);
    if (normalized.length > 0) {
      unique.add(normalized);
    }
  }

  return [...unique];
};

const isPluginInvokeResponse = (value: unknown): value is PluginInvokeResponse => {
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

const isMServiceErrorCode = (value: string): value is MServiceErrorCode =>
  M_SERVICE_ERROR_CODES.has(value as MServiceErrorCode);

const isMServiceResponse = (value: unknown): value is MServiceResponse => {
  if (!isRecord(value) || typeof value.success !== 'boolean') {
    return false;
  }

  if (!value.success) {
    if (!isRecord(value.error)) {
      return false;
    }

    return (
      typeof value.error.code === 'string' &&
      isMServiceErrorCode(value.error.code) &&
      typeof value.error.message === 'string'
    );
  }

  return true;
};

class RouterTimeoutError extends Error {
  public constructor(message: string) {
    super(message);
    this.name = 'RouterTimeoutError';
  }
}

export class ServiceRegistry {
  private readonly services = new Map<string, StoredServiceRegistration>();
  private readonly pluginServices = new Map<string, Set<string>>();

  public register(service: string, pluginId: string, methods: string[]): void {
    const normalizedService = normalizeName(service);
    const normalizedPluginId = normalizeName(pluginId);

    if (normalizedService.length === 0) {
      throw new Error('M-Service name must be non-empty');
    }

    if (normalizedPluginId.length === 0) {
      throw new Error('M-Service pluginId must be non-empty');
    }

    const nextRegistration: StoredServiceRegistration = {
      service: normalizedService,
      pluginId: normalizedPluginId,
      methods: normalizeMethods(methods),
    };

    const previousRegistration = this.services.get(normalizedService);
    if (previousRegistration) {
      this.removePluginService(previousRegistration.pluginId, normalizedService);
    }

    this.services.set(normalizedService, nextRegistration);
    this.linkPluginService(normalizedPluginId, normalizedService);
  }

  public unregister(service: string): void {
    const normalizedService = normalizeName(service);
    const existing = this.services.get(normalizedService);
    if (!existing) {
      return;
    }

    this.services.delete(normalizedService);
    this.removePluginService(existing.pluginId, normalizedService);
  }

  public lookup(service: string): ServiceRegistration | undefined {
    const normalizedService = normalizeName(service);
    const registration = this.services.get(normalizedService);
    if (!registration) {
      return undefined;
    }

    return {
      service: registration.service,
      pluginId: registration.pluginId,
      methods: [...registration.methods],
    };
  }

  public getPluginServices(pluginId: string): string[] {
    const normalizedPluginId = normalizeName(pluginId);
    const services = this.pluginServices.get(normalizedPluginId);
    if (!services) {
      return [];
    }

    return [...services].sort();
  }

  private linkPluginService(pluginId: string, service: string): void {
    const existing = this.pluginServices.get(pluginId);
    if (existing) {
      existing.add(service);
      return;
    }

    this.pluginServices.set(pluginId, new Set([service]));
  }

  private removePluginService(pluginId: string, service: string): void {
    const existing = this.pluginServices.get(pluginId);
    if (!existing) {
      return;
    }

    existing.delete(service);
    if (existing.size === 0) {
      this.pluginServices.delete(pluginId);
    }
  }
}

export type MServiceRouterOptions = Readonly<{
  registry?: ServiceRegistry;
  messageBridge?: MessageBridge;
  resolveWorker?: WorkerResolver;
  defaultTimeoutMs?: number;
}>;

export class MServiceRouter {
  private readonly registry: ServiceRegistry;
  private readonly messageBridge: MessageBridge;
  private readonly resolveWorker: WorkerResolver;
  private readonly defaultTimeoutMs: number;

  public constructor(options: MServiceRouterOptions = {}) {
    this.registry = options.registry ?? new ServiceRegistry();
    this.messageBridge = options.messageBridge ?? new MessageBridge();
    this.resolveWorker = options.resolveWorker ?? (() => undefined);
    this.defaultTimeoutMs = this.normalizeTimeoutMs(
      options.defaultTimeoutMs,
      DEFAULT_TIMEOUT_MS
    );
  }

  /**
   * 逻辑块：M-Service 路由主流程。
   * - 目的：在 Core 内完成权限、服务发现、目标 Worker 转发与错误统一。
   * - 原因：插件互调必须经过统一入口，确保快路径和总线语义一致。
   * - 失败路径：权限不足返回 ACCESS_DENIED；超时返回 TIMEOUT；其余异常统一降级 SERVICE_UNAVAILABLE。
   */
  public async route(request: MServiceRequest): Promise<MServiceResponse> {
    try {
      requirePermission(request.caller, 'plugin:access');
    } catch (error) {
      return this.createErrorResponse(
        'ACCESS_DENIED',
        error instanceof Error
          ? error.message
          : `Plugin ${request.caller} lacks plugin:access permission`
      );
    }

    const registration = this.registry.lookup(request.service);
    if (!registration) {
      return this.createErrorResponse(
        'SERVICE_UNAVAILABLE',
        `M-Service ${request.service} is not registered`
      );
    }

    if (
      registration.methods.length > 0 &&
      !registration.methods.includes(normalizeName(request.method))
    ) {
      return this.createErrorResponse(
        'METHOD_NOT_FOUND',
        `M-Service method not found: ${request.service}.${request.method}`
      );
    }

    const targetWorker = this.resolveWorker(registration.pluginId);
    if (!targetWorker) {
      return this.createErrorResponse(
        'SERVICE_UNAVAILABLE',
        `Target plugin ${registration.pluginId} is not running`
      );
    }

    const timeoutMs = this.resolveTimeoutMs(request.timeout);

    try {
      const response = await this.withTimeout(
        this.messageBridge.sendMessageAndWait(
          targetWorker,
          {
            id: this.messageBridge.generateMessageId(),
            type: PluginMessageType.INVOKE,
            pluginId: registration.pluginId,
            timestamp: Date.now(),
            traceId: request.trace_id,
            payload: {
              method: `${request.service}.${request.method}`,
              params: {
                trace_id: request.trace_id,
                caller: request.caller,
                service: request.service,
                method: request.method,
                payload: request.payload,
              },
              timeout: timeoutMs,
            },
          },
          timeoutMs + BRIDGE_TIMEOUT_BUFFER_MS
        ),
        timeoutMs,
        request
      );

      return this.normalizeResponsePayload(request, response.payload);
    } catch (error) {
      if (error instanceof RouterTimeoutError || this.isTimeoutError(error)) {
        return this.createErrorResponse(
          'TIMEOUT',
          `M-Service ${request.service}.${request.method} timed out after ${timeoutMs}ms`
        );
      }

      return this.createErrorResponse(
        'SERVICE_UNAVAILABLE',
        error instanceof Error
          ? error.message
          : `M-Service ${request.service}.${request.method} is unavailable`
      );
    }
  }

  /**
   * 逻辑块：超时门禁包装。
   * - 目的：强制所有跨插件调用遵循统一超时上限，避免调用方无限阻塞。
   * - 原因：被调插件可能卡死或 Worker 失联，必须由路由层兜底超时。
   * - 失败路径：超过超时阈值抛 RouterTimeoutError，上层统一映射 TIMEOUT。
   */
  private async withTimeout<T>(
    promise: Promise<T>,
    timeoutMs: number,
    request: MServiceRequest
  ): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;

    try {
      return await Promise.race([
        promise,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            reject(
              new RouterTimeoutError(
                `M-Service ${request.service}.${request.method} timed out after ${timeoutMs}ms`
              )
            );
          }, timeoutMs);
        }),
      ]);
    } finally {
      if (timer) {
        clearTimeout(timer);
      }
    }
  }

  private normalizeResponsePayload(
    request: MServiceRequest,
    payload: unknown
  ): MServiceResponse {
    if (isMServiceResponse(payload)) {
      return payload;
    }

    if (isPluginInvokeResponse(payload)) {
      if (payload.success) {
        return {
          success: true,
          data: payload.data,
        };
      }

      return this.createErrorResponse(
        this.normalizeErrorCode(payload.error?.code),
        payload.error?.message ??
          `M-Service ${request.service}.${request.method} failed in target plugin`
      );
    }

    return {
      success: true,
      data: payload,
    };
  }

  private normalizeErrorCode(code: string | undefined): MServiceErrorCode {
    if (typeof code !== 'string') {
      return 'SERVICE_UNAVAILABLE';
    }

    if (code === 'PERMISSION_DENIED') {
      return 'ACCESS_DENIED';
    }

    if (isMServiceErrorCode(code)) {
      return code;
    }

    return 'SERVICE_UNAVAILABLE';
  }

  private isTimeoutError(error: unknown): boolean {
    if (!(error instanceof Error)) {
      return false;
    }

    return error.message.toLowerCase().includes('timed out');
  }

  private resolveTimeoutMs(timeoutMs: number | undefined): number {
    return this.normalizeTimeoutMs(timeoutMs, this.defaultTimeoutMs);
  }

  private normalizeTimeoutMs(timeoutMs: number | undefined, fallback: number): number {
    if (
      typeof timeoutMs !== 'number' ||
      !Number.isFinite(timeoutMs) ||
      timeoutMs <= 0
    ) {
      return fallback;
    }

    return Math.floor(timeoutMs);
  }

  private createErrorResponse(
    code: MServiceErrorCode,
    message: string
  ): MServiceResponse {
    return {
      success: false,
      error: {
        code,
        message,
      },
    };
  }
}

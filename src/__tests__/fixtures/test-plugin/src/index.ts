import {
  PluginMessageType,
  type PluginHealthReport,
  type PluginInvokeRequest,
  type PluginInvokeResponse,
  type PluginMessage,
} from '@insnh-gd/meristem-shared';

type PluginControlMessage = Readonly<{
  type: 'TEST_CONTROL';
  command: 'release-reload-start' | 'emit-health';
  rss?: number;
  status?: PluginHealthReport['status'];
}>;

type RuntimeState = {
  pluginId: string;
  started: boolean;
  reloadStartBlocked: boolean;
  reloadStartResolver: (() => void) | null;
  runtimeId: string;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

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

const isInvokeRequest = (value: unknown): value is PluginInvokeRequest => {
  if (!isRecord(value)) {
    return false;
  }

  return typeof value.method === 'string' && 'params' in value;
};

const isControlMessage = (value: unknown): value is PluginControlMessage => {
  if (!isRecord(value) || value.type !== 'TEST_CONTROL') {
    return false;
  }

  return value.command === 'release-reload-start' || value.command === 'emit-health';
};

const state: RuntimeState = {
  pluginId: 'unknown',
  started: false,
  reloadStartBlocked: false,
  reloadStartResolver: null,
  runtimeId: `runtime-${Date.now()}-${Math.random().toString(16).slice(2)}`,
};

const sleep = async (ms: number): Promise<void> => {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
};

const readPayload = (params: unknown): Record<string, unknown> => {
  if (!isRecord(params)) {
    return {};
  }

  return isRecord(params.payload) ? params.payload : {};
};

const createHealthReport = (
  rss?: number,
  status: PluginHealthReport['status'] = 'healthy',
): PluginHealthReport => {
  const usage = process.memoryUsage();
  return {
    memoryUsage: {
      ...usage,
      rss: typeof rss === 'number' && Number.isFinite(rss) ? rss : usage.rss,
    },
    uptime: process.uptime(),
    status,
  };
};

const postHealth = (rss?: number, status: PluginHealthReport['status'] = 'healthy'): void => {
  const message: PluginMessage = {
    id: crypto.randomUUID(),
    type: PluginMessageType.HEALTH,
    pluginId: state.pluginId,
    timestamp: Date.now(),
    payload: createHealthReport(rss, status),
  };
  globalThis.postMessage(message);
};

const createInvokeResult = (
  request: PluginMessage,
  response: PluginInvokeResponse,
): PluginMessage => ({
  id: request.id,
  type: PluginMessageType.INVOKE_RESULT,
  pluginId: state.pluginId,
  timestamp: Date.now(),
  traceId: request.traceId,
  payload: response,
});

export const onInit = async (_params: unknown): Promise<{ hook: string }> => ({
  hook: 'onInit',
});

export const onStart = async (params: unknown): Promise<{ hook: string; runtimeId: string }> => {
  const reload = isRecord(params) && params.reload === true;
  if (reload && state.reloadStartBlocked) {
    await new Promise<void>((resolve) => {
      state.reloadStartResolver = resolve;
    });
  }

  state.started = true;
  return {
    hook: 'onStart',
    runtimeId: state.runtimeId,
  };
};

export const onStop = async (_params: unknown): Promise<{ hook: string }> => {
  state.started = false;
  return { hook: 'onStop' };
};

export const onDestroy = async (_params: unknown): Promise<{ hook: string }> => ({
  hook: 'onDestroy',
});

const invokeService = async (method: string, params: unknown): Promise<unknown> => {
  const payload = readPayload(params);

  if (method.endsWith('.profile.get')) {
    return {
      profileId: typeof payload.profileId === 'string' ? payload.profileId : 'unknown',
      servedBy: state.runtimeId,
      started: state.started,
    };
  }

  if (method.endsWith('.slow.wait')) {
    const delayMs =
      typeof payload.delayMs === 'number' && Number.isFinite(payload.delayMs)
        ? Math.max(0, Math.floor(payload.delayMs))
        : 0;
    await sleep(delayMs);
    return {
      done: true,
      servedBy: state.runtimeId,
    };
  }

  if (method.endsWith('.runtime.identity')) {
    return {
      runtimeId: state.runtimeId,
      started: state.started,
    };
  }

  throw new Error(`METHOD_NOT_FOUND:${method}`);
};

const handleInvoke = async (request: PluginInvokeRequest): Promise<PluginInvokeResponse> => {
  if (request.method === 'onInit') {
    return { success: true, data: await onInit(request.params) };
  }

  if (request.method === 'onStart') {
    return { success: true, data: await onStart(request.params) };
  }

  if (request.method === 'onStop') {
    return { success: true, data: await onStop(request.params) };
  }

  if (request.method === 'onDestroy') {
    return { success: true, data: await onDestroy(request.params) };
  }

  try {
    return {
      success: true,
      data: await invokeService(request.method, request.params),
    };
  } catch (error) {
    return {
      success: false,
      error: {
        code: 'METHOD_NOT_FOUND',
        message: error instanceof Error ? error.message : String(error),
      },
    };
  }
};

const handleControl = (message: PluginControlMessage): void => {
  if (message.command === 'release-reload-start') {
    state.reloadStartBlocked = false;
    state.reloadStartResolver?.();
    state.reloadStartResolver = null;
    return;
  }

  postHealth(message.rss, message.status ?? 'healthy');
};

const onMessage = (event: MessageEvent<unknown>): void => {
  const payload = event.data;

  if (isControlMessage(payload)) {
    handleControl(payload);
    return;
  }

  if (isRecord(payload) && payload.type === 'HEALTH') {
    postHealth();
    return;
  }

  if (!isPluginMessage(payload)) {
    return;
  }

  if (payload.type === PluginMessageType.INIT) {
    state.pluginId = payload.pluginId;
    return;
  }

  if (payload.type === PluginMessageType.TERMINATE) {
    state.started = false;
    return;
  }

  if (payload.type !== PluginMessageType.INVOKE || !isInvokeRequest(payload.payload)) {
    return;
  }

  void handleInvoke(payload.payload).then((result) => {
    globalThis.postMessage(createInvokeResult(payload, result));
  });
};

self.addEventListener('message', onMessage);

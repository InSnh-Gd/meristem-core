import type { Elysia } from 'elysia';
import type {
  WsErrorCode,
  WsServerMessage as SharedWsServerMessage,
  WsTopic,
} from '@insnh-gd/meristem-shared';
import { loadConfig } from '../config';
import { verifyJwtToken } from '../middleware/auth';
import { createTraceContext, generateTraceId } from '../utils/trace-context';
import { createLogger } from '../utils/logger';
import { evaluateSubjectPermission } from '../services/subject-permission-guard';

type WsStreamProfilePreset = 'realtime' | 'balanced' | 'conserve';

type WsStreamProfile = Readonly<{
  preset: WsStreamProfilePreset;
  min_interval_ms: number;
  debounce_ms: number;
  batch_window_ms: number;
  batch_max_size: number;
}>;

export type WsMessageType = 'SUBSCRIBE' | 'UNSUBSCRIBE' | 'PING';

export type WsClientMessage =
  | {
      type: 'SUBSCRIBE';
      topic: string;
      stream_profile?: WsStreamProfilePreset | Partial<WsStreamProfile>;
    }
  | {
      type: 'UNSUBSCRIBE';
      topic: string;
    }
  | {
      type: 'PING';
    };

export type WsServerMessage = SharedWsServerMessage;

export type WsConnection = {
  id?: string | number;
  data?: {
    query?: Record<string, unknown>;
  };
  send: (message: string | Uint8Array) => unknown;
  close?: () => unknown;
};

export type WsHandlers = {
  open: (ws: WsConnection) => void;
  message: (ws: WsConnection, message: unknown) => void;
  close: (ws: WsConnection) => void;
};

export type WsManager = {
  connect: (ws: WsConnection, token: string | undefined) => Promise<boolean>;
  handleMessage: (ws: WsConnection, rawMessage: unknown) => void;
  disconnect: (ws: WsConnection) => void;
  broadcast: (topic: string, payload: unknown) => number;
};

export type WsRouteOptions = {
  wsPath?: string;
  manager?: WsManager;
  validateToken?: (token: string) => Promise<WsAuthContext | null>;
  enableEdenSubscribe?: boolean;
};

export type WsAuthContext = {
  subject: string;
  permissions: readonly string[];
  traceId: string;
  allowedTopics?: readonly string[];
};

type WsRegistrableApp = {
  ws: (path: string, handlers: WsHandlers) => unknown;
};

const TOPIC_PATTERN_NODE_STATUS = /^node\.[^.]+\.status$/;
const TOPIC_PATTERN_TASK_STATUS = /^task\.[^.]+\.status$/;
const MESSAGE_DECODER = new TextDecoder();
const WS_STREAM_PROFILE_PRESETS: Readonly<Record<WsStreamProfilePreset, WsStreamProfile>> = Object.freeze({
  realtime: Object.freeze({
    preset: 'realtime',
    min_interval_ms: 0,
    debounce_ms: 0,
    batch_window_ms: 0,
    batch_max_size: 1,
  }),
  balanced: Object.freeze({
    preset: 'balanced',
    min_interval_ms: 120,
    debounce_ms: 80,
    batch_window_ms: 150,
    batch_max_size: 10,
  }),
  conserve: Object.freeze({
    preset: 'conserve',
    min_interval_ms: 500,
    debounce_ms: 300,
    batch_window_ms: 400,
    batch_max_size: 20,
  }),
});

const isStringArray = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((item) => typeof item === 'string');

const isWsStreamProfilePreset = (value: unknown): value is WsStreamProfilePreset =>
  value === 'realtime' || value === 'balanced' || value === 'conserve';

const toNonNegativeInt = (value: unknown, fallback: number): number => {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return fallback;
  }
  if (value < 0) {
    return fallback;
  }
  return Math.floor(value);
};

const resolveWsStreamProfile = (input: unknown): WsStreamProfile => {
  if (isWsStreamProfilePreset(input)) {
    return WS_STREAM_PROFILE_PRESETS[input];
  }

  if (!isRecord(input)) {
    return WS_STREAM_PROFILE_PRESETS.balanced;
  }

  const preset = isWsStreamProfilePreset(input.preset) ? input.preset : 'balanced';
  const base = WS_STREAM_PROFILE_PRESETS[preset];
  return Object.freeze({
    preset,
    min_interval_ms: toNonNegativeInt(input.min_interval_ms, base.min_interval_ms),
    debounce_ms: toNonNegativeInt(input.debounce_ms, base.debounce_ms),
    batch_window_ms: toNonNegativeInt(input.batch_window_ms, base.batch_window_ms),
    batch_max_size: toNonNegativeInt(input.batch_max_size, base.batch_max_size),
  });
};

const defaultTokenValidator = async (token: string): Promise<WsAuthContext | null> => {
  if (token.trim().length === 0) {
    return null;
  }

  const traceId = generateTraceId();
  const payload = await verifyJwtToken(
    createTraceContext({
      traceId,
      nodeId: 'core',
      source: 'ws-auth',
    }),
    token,
  );
  if (!payload || typeof payload.sub !== 'string' || payload.sub.length === 0) {
    return null;
  }

  const payloadRecord = payload as unknown as Record<string, unknown>;
  const permissions = isStringArray(payloadRecord.permissions) ? payloadRecord.permissions : [];
  const allowedTopicsFromToken = isStringArray(payloadRecord.ui_channels)
    ? payloadRecord.ui_channels
    : isStringArray(payloadRecord.allowed_topics)
      ? payloadRecord.allowed_topics
      : undefined;

  return {
    subject: payload.sub,
    permissions,
    traceId,
    allowedTopics: allowedTopicsFromToken,
  };
};

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === 'object' && value !== null;
};

const normalizeRawMessage = (value: unknown): string | null => {
  if (typeof value === 'string') {
    return value;
  }

  if (value instanceof Uint8Array) {
    return MESSAGE_DECODER.decode(value);
  }

  return null;
};

const parseClientMessage = (raw: unknown): WsClientMessage | null => {
  const normalized = normalizeRawMessage(raw);
  if (!normalized) {
    return null;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(normalized);
  } catch {
    return null;
  }

  if (!isRecord(parsed)) {
    return null;
  }

  const type = parsed.type;
  if (type === 'PING') {
    return { type: 'PING' };
  }

  const topic = parsed.topic;
  if (typeof topic !== 'string' || topic.trim().length === 0) {
    return null;
  }

  if (type === 'SUBSCRIBE') {
    const streamProfile = parsed.stream_profile;
    if (streamProfile !== undefined && typeof streamProfile !== 'string' && !isRecord(streamProfile)) {
      return null;
    }
    return {
      type: 'SUBSCRIBE',
      topic,
      stream_profile: streamProfile as WsStreamProfilePreset | Partial<WsStreamProfile> | undefined,
    };
  }

  if (type === 'UNSUBSCRIBE') {
    return { type: 'UNSUBSCRIBE', topic };
  }

  return null;
};

const getTokenFromConnection = (ws: WsConnection): string | undefined => {
  const token = ws.data?.query?.token;

  if (typeof token === 'string') {
    return token;
  }

  if (Array.isArray(token) && token.length > 0 && typeof token[0] === 'string') {
    return token[0];
  }

  return undefined;
};

const getTopicFromConnection = (ws: WsConnection): string | undefined => {
  const topic = ws.data?.query?.topic;

  if (typeof topic === 'string' && topic.trim().length > 0) {
    return topic;
  }

  if (Array.isArray(topic) && topic.length > 0 && typeof topic[0] === 'string' && topic[0].trim().length > 0) {
    return topic[0];
  }

  return undefined;
};

const sendServerMessage = (ws: WsConnection, message: WsServerMessage): void => {
  ws.send(JSON.stringify(message));
};

const sendAck = (
  ws: WsConnection,
  action: 'CONNECTED' | 'SUBSCRIBE' | 'UNSUBSCRIBE' | 'PONG',
  topic?: string,
  streamProfile?: WsStreamProfilePreset,
): void => {
  const payload: WsServerMessage = {
    type: 'ACK',
    action,
    ...(topic ? { topic } : {}),
    ...(streamProfile ? { stream_profile: streamProfile } : {}),
  };
  sendServerMessage(ws, payload);
};

const sendError = (
  ws: WsConnection,
  code: WsErrorCode,
  message: string,
): void => {
  sendServerMessage(ws, {
    type: 'ERROR',
    code,
    error: code,
    message,
  });
};

export const createWebSocketManager = (
  validateToken: (token: string) => Promise<WsAuthContext | null> = defaultTokenValidator,
): WsManager => {
  const connectionIds = new WeakMap<WsConnection, string>();
  const connections = new Map<string, WsConnection>();
  const topics = new Map<string, Set<string>>();
  const authContexts = new Map<string, WsAuthContext>();
  const subscriptionProfiles = new Map<string, WsStreamProfile>();
  const lastDeliveredAtBySubscription = new Map<string, number>();
  let idSequence = 0;
  const defaultStreamProfile = resolveWsStreamProfile(undefined);

  const getSubscriptionKey = (connectionId: string, topic: string): string => `${connectionId}::${topic}`;

  const auditDeniedSubscription = (
    authContext: WsAuthContext,
    topic: string,
    requiredPermission: string | null,
    reason: string,
  ): void => {
    createLogger(
      createTraceContext({
        traceId: authContext.traceId,
        nodeId: 'core',
        source: 'ws-subscription-guard',
      }),
    ).warn('WebSocket subscription denied by core guard', {
      event: 'WS_SUBSCRIPTION_DENIED',
      actor: authContext.subject,
      topic,
      required_permission: requiredPermission,
      reason,
    });
  };

  const resolveConnectionId = (ws: WsConnection): string => {
    const known = connectionIds.get(ws);
    if (known) {
      return known;
    }

    if (typeof ws.id === 'string' && ws.id.length > 0) {
      connectionIds.set(ws, ws.id);
      return ws.id;
    }

    if (typeof ws.id === 'number' && Number.isFinite(ws.id)) {
      const numericId = String(ws.id);
      connectionIds.set(ws, numericId);
      return numericId;
    }

    idSequence += 1;
    const generated = `ws-${idSequence}`;
    connectionIds.set(ws, generated);
    return generated;
  };

  const cleanupConnectionTopics = (connectionId: string): void => {
    for (const [topic, subscribers] of topics.entries()) {
      subscribers.delete(connectionId);
      const subscriptionKey = getSubscriptionKey(connectionId, topic);
      subscriptionProfiles.delete(subscriptionKey);
      lastDeliveredAtBySubscription.delete(subscriptionKey);
      if (subscribers.size === 0) {
        topics.delete(topic);
      }
    }
  };

  const connect = async (ws: WsConnection, token: string | undefined): Promise<boolean> => {
    if (!token) {
      sendError(ws, 'AUTH_REQUIRED', 'Missing token query parameter');
      return false;
    }

    const authContext = await validateToken(token);
    if (!authContext) {
      sendError(ws, 'AUTH_INVALID', 'Invalid token');
      return false;
    }

    const connectionId = resolveConnectionId(ws);
    connections.set(connectionId, ws);
    authContexts.set(connectionId, authContext);
    sendAck(ws, 'CONNECTED');
    return true;
  };

  const disconnect = (ws: WsConnection): void => {
    const connectionId = resolveConnectionId(ws);
    cleanupConnectionTopics(connectionId);
    connections.delete(connectionId);
    authContexts.delete(connectionId);
  };

  const handleSubscribe = (
    ws: WsConnection,
    topic: string,
    streamProfileInput?: WsStreamProfilePreset | Partial<WsStreamProfile>,
  ): void => {
    if (topic.trim().length === 0) {
      sendError(ws, 'INVALID_TOPIC', 'Topic must not be empty');
      return;
    }

    const connectionId = resolveConnectionId(ws);
    const authContext = authContexts.get(connectionId);
    if (!authContext) {
      sendError(ws, 'NOT_CONNECTED', 'Connection is not authenticated');
      return;
    }

    if (!TOPIC_PATTERN_TASK_STATUS.test(topic) && !TOPIC_PATTERN_NODE_STATUS.test(topic)) {
      sendError(ws, 'INVALID_TOPIC', 'Topic is not allowed');
      return;
    }

    /**
     * 逻辑块：前端订阅必须遵循 UI 契约白名单。
     * - 目的：保证“前端非插件运行时”边界下，UI 仅能访问 manifest 声明的频道。
     * - 原因：避免 WebSocket 被滥用为越权总线入口。
     * - 降级：白名单未声明主题时直接拒绝并记录审计，不做隐式放行。
     */
    if (authContext.allowedTopics && !authContext.allowedTopics.includes(topic)) {
      auditDeniedSubscription(authContext, topic, null, 'DENY_UI_CONTRACT');
      sendError(ws, 'INVALID_TOPIC', 'Topic is not allowed');
      return;
    }

    const permissionDecision = evaluateSubjectPermission({
      subject: topic,
      permissions: authContext.permissions,
    });
    if (!permissionDecision.allowed) {
      auditDeniedSubscription(authContext, topic, permissionDecision.requiredPermission, permissionDecision.reason);
      sendError(ws, 'INVALID_TOPIC', 'Topic is not allowed');
      return;
    }

    let subscribers = topics.get(topic);

    if (!subscribers) {
      subscribers = new Set<string>();
      topics.set(topic, subscribers);
    }

    const streamProfile = resolveWsStreamProfile(streamProfileInput);
    const subscriptionKey = getSubscriptionKey(connectionId, topic);
    subscribers.add(connectionId);
    subscriptionProfiles.set(subscriptionKey, streamProfile);
    sendAck(ws, 'SUBSCRIBE', topic, streamProfile.preset);
  };

  const handleUnsubscribe = (ws: WsConnection, topic: string): void => {
    const connectionId = resolveConnectionId(ws);
    const subscribers = topics.get(topic);
    const subscriptionKey = getSubscriptionKey(connectionId, topic);

    if (subscribers) {
      subscribers.delete(connectionId);
      if (subscribers.size === 0) {
        topics.delete(topic);
      }
    }
    subscriptionProfiles.delete(subscriptionKey);
    lastDeliveredAtBySubscription.delete(subscriptionKey);

    sendAck(ws, 'UNSUBSCRIBE', topic);
  };

  const handleMessage = (ws: WsConnection, rawMessage: unknown): void => {
    const connectionId = resolveConnectionId(ws);
    if (!connections.has(connectionId)) {
      sendError(ws, 'NOT_CONNECTED', 'Connection is not authenticated');
      return;
    }

    const message = parseClientMessage(rawMessage);
    if (!message) {
      sendError(ws, 'INVALID_MESSAGE', 'Invalid message payload');
      return;
    }

    switch (message.type) {
      case 'SUBSCRIBE':
        handleSubscribe(ws, message.topic, message.stream_profile);
        return;
      case 'UNSUBSCRIBE':
        handleUnsubscribe(ws, message.topic);
        return;
      case 'PING':
        sendAck(ws, 'PONG');
        return;
      default:
        sendError(ws, 'INVALID_MESSAGE', 'Unsupported message type');
    }
  };

  const broadcast = (topic: string, payload: unknown): number => {
    const subscribers = topics.get(topic);
    if (!subscribers || subscribers.size === 0) {
      return 0;
    }

    let sent = 0;
    for (const connectionId of subscribers) {
      const ws = connections.get(connectionId);
      const authContext = authContexts.get(connectionId);
      if (!ws) {
        continue;
      }

      if (!authContext) {
        continue;
      }

      const subscriptionKey = getSubscriptionKey(connectionId, topic);
      const streamProfile = subscriptionProfiles.get(subscriptionKey) ?? defaultStreamProfile;
      const now = Date.now();
      const lastDeliveredAt = lastDeliveredAtBySubscription.get(subscriptionKey) ?? 0;

      /**
       * 逻辑块：Phase 2.5 先落地最小降频门禁（throttle）。
       * - 目的：限制高频 PUSH 对前端渲染线程与 WS 带宽的瞬时冲击。
       * - 原因：在 Phase 4 完整优化前，先用契约化档位提供可控上限。
       * - 降级：超过频率预算时直接跳过当前帧，等待下一次广播。
       */
      if (streamProfile.min_interval_ms > 0 && now - lastDeliveredAt < streamProfile.min_interval_ms) {
        continue;
      }

      const encoded = JSON.stringify({
        type: 'PUSH',
        topic: topic as WsTopic,
        payload,
        trace_id: authContext.traceId,
      } satisfies WsServerMessage);

      ws.send(encoded);
      lastDeliveredAtBySubscription.set(subscriptionKey, now);
      sent += 1;
    }

    return sent;
  };

  return {
    connect,
    handleMessage,
    disconnect,
    broadcast,
  };
};

let activeWsManager: WsManager | null = null;

export const broadcastWsPush = (topic: string, payload: unknown): number => {
  if (!activeWsManager) {
    return 0;
  }

  return activeWsManager.broadcast(topic, payload);
};

export const createWsHandlers = (
  manager: WsManager,
  options: {
    enableEdenSubscribe?: boolean;
  } = {},
): WsHandlers => {
  const enableEdenSubscribe = options.enableEdenSubscribe ?? false;
  return {
    open: (ws) => {
      const token = getTokenFromConnection(ws);
      void manager.connect(ws, token).then((accepted) => {
        if (!accepted) {
          ws.close?.();
          return;
        }
        if (!enableEdenSubscribe) {
          return;
        }
        const topic = getTopicFromConnection(ws);
        if (!topic) {
          return;
        }
        manager.handleMessage(ws, JSON.stringify({ type: 'SUBSCRIBE', topic }));
      });
    },
    message: (ws, rawMessage) => {
      manager.handleMessage(ws, rawMessage);
    },
    close: (ws) => {
      manager.disconnect(ws);
    },
  };
};

export const wsRoute = (app: Elysia, options: WsRouteOptions = {}): Elysia => {
  const manager = options.manager ?? createWebSocketManager(options.validateToken);
  const wsPath = options.wsPath ?? loadConfig().server.ws_path;
  const registrable = app as unknown as WsRegistrableApp;

  registrable.ws(
    wsPath,
    createWsHandlers(manager, {
      enableEdenSubscribe: options.enableEdenSubscribe,
    }),
  );
  activeWsManager = manager;

  return app;
};

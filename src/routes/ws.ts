import type { Elysia } from 'elysia';
import { loadConfig } from '../config';
import { verifyJwtToken } from '../middleware/auth';
import { createTraceContext, generateTraceId } from '../utils/trace-context';

export type WsMessageType = 'SUBSCRIBE' | 'UNSUBSCRIBE' | 'PING';

export type WsClientMessage =
  | {
      type: 'SUBSCRIBE';
      topic: string;
    }
  | {
      type: 'UNSUBSCRIBE';
      topic: string;
    }
  | {
      type: 'PING';
    };

export type WsServerMessage =
  | {
      type: 'ACK';
      action: 'CONNECTED' | 'SUBSCRIBE' | 'UNSUBSCRIBE' | 'PONG';
      topic?: string;
    }
  | {
      type: 'ERROR';
      code: 'AUTH_REQUIRED' | 'AUTH_INVALID' | 'NOT_CONNECTED' | 'INVALID_MESSAGE' | 'INVALID_TOPIC';
      message: string;
    }
  | {
      type: 'PUSH';
      topic: string;
      payload: unknown;
      trace_id: string;
    };

export type WsConnection = {
  id?: string | number;
  data?: {
    query?: Record<string, unknown>;
  };
  send: (message: string) => unknown;
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
};

export type WsAuthContext = {
  subject: string;
  permissions: readonly string[];
  traceId: string;
};

type WsRegistrableApp = {
  ws: (path: string, handlers: WsHandlers) => unknown;
};

const TOPIC_PATTERN_NODE_STATUS = /^node\.[^.]+\.status$/;
const TOPIC_PATTERN_TASK_STATUS = /^task\.[^.]+\.status$/;

const isStringArray = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((item) => typeof item === 'string');

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

  const permissions = isStringArray(payload.permissions) ? payload.permissions : [];
  return {
    subject: payload.sub,
    permissions,
    traceId,
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
    return new TextDecoder().decode(value);
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
    return { type: 'SUBSCRIBE', topic };
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

const sendServerMessage = (ws: WsConnection, message: WsServerMessage): void => {
  ws.send(JSON.stringify(message));
};

const sendAck = (
  ws: WsConnection,
  action: 'CONNECTED' | 'SUBSCRIBE' | 'UNSUBSCRIBE' | 'PONG',
  topic?: string,
): void => {
  const payload: WsServerMessage = topic
    ? {
        type: 'ACK',
        action,
        topic,
      }
    : {
        type: 'ACK',
        action,
      };
  sendServerMessage(ws, payload);
};

const sendError = (
  ws: WsConnection,
  code: 'AUTH_REQUIRED' | 'AUTH_INVALID' | 'NOT_CONNECTED' | 'INVALID_MESSAGE' | 'INVALID_TOPIC',
  message: string,
): void => {
  sendServerMessage(ws, {
    type: 'ERROR',
    code,
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
  let idSequence = 0;

  const isAllowedTopic = (topic: string, permissions: readonly string[]): boolean => {
    if (TOPIC_PATTERN_TASK_STATUS.test(topic)) {
      return true;
    }

    if (TOPIC_PATTERN_NODE_STATUS.test(topic)) {
      return permissions.includes('node:read') || permissions.includes('sys:audit') || permissions.includes('*');
    }

    return false;
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

  const handleSubscribe = (ws: WsConnection, topic: string): void => {
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

    if (!isAllowedTopic(topic, authContext.permissions)) {
      sendError(ws, 'INVALID_TOPIC', 'Topic is not allowed');
      return;
    }

    let subscribers = topics.get(topic);

    if (!subscribers) {
      subscribers = new Set<string>();
      topics.set(topic, subscribers);
    }

    subscribers.add(connectionId);
    sendAck(ws, 'SUBSCRIBE', topic);
  };

  const handleUnsubscribe = (ws: WsConnection, topic: string): void => {
    const connectionId = resolveConnectionId(ws);
    const subscribers = topics.get(topic);

    if (subscribers) {
      subscribers.delete(connectionId);
      if (subscribers.size === 0) {
        topics.delete(topic);
      }
    }

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
        handleSubscribe(ws, message.topic);
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

      const encoded = JSON.stringify({
        type: 'PUSH',
        topic,
        payload,
        trace_id: authContext.traceId,
      } satisfies WsServerMessage);

      ws.send(encoded);
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

export const createWsHandlers = (manager: WsManager): WsHandlers => {
  return {
    open: (ws) => {
      const token = getTokenFromConnection(ws);
      void manager.connect(ws, token).then((accepted) => {
        if (!accepted) {
          ws.close?.();
        }
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

  registrable.ws(wsPath, createWsHandlers(manager));
  activeWsManager = manager;

  return app;
};

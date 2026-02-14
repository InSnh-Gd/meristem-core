import { afterEach, expect, test } from 'bun:test';
import type { Elysia } from 'elysia';
import { SignJWT } from 'jose';
import {
  createWebSocketManager,
  wsRoute,
  type WsConnection,
  type WsHandlers,
  type WsAuthContext,
} from '../routes/ws';

const originalMeristemJwtSignSecret = process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;
const originalMeristemJwtVerifySecrets = process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS;

afterEach((): void => {
  if (originalMeristemJwtSignSecret === undefined) {
    delete process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET;
  } else {
    process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = originalMeristemJwtSignSecret;
  }

  if (originalMeristemJwtVerifySecrets === undefined) {
    delete process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS;
  } else {
    process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS = originalMeristemJwtVerifySecrets;
  }
});

const allowToken = async (token: string): Promise<WsAuthContext | null> => {
  if (!token.startsWith('valid')) {
    return null;
  }

  return {
    subject: token,
    permissions: ['node:read'],
    traceId: `trace-${token}`,
    allowedTopics: ['task.1.status', 'task.eden.status', 'node.a.status'],
  };
};

const createMockConnection = (
  id: string,
  token?: string,
  queryOverrides: Record<string, unknown> = {},
): {
  connection: WsConnection;
  sent: string[];
  closeCalls: number;
} => {
  const sent: string[] = [];
  let closeCalls = 0;

  const connection: WsConnection = {
    id,
    data: {
      query:
        token !== undefined
          ? {
              token,
              ...queryOverrides,
            }
          : queryOverrides,
    },
    send: (message: string | Uint8Array): void => {
      if (typeof message === 'string') {
        sent.push(message);
        return;
      }
      sent.push(new TextDecoder().decode(message));
    },
    close: (): void => {
      closeCalls += 1;
    },
  };

  return {
    connection,
    sent,
    get closeCalls(): number {
      return closeCalls;
    },
  };
};

test('websocket manager rejects missing or invalid token on connect', async (): Promise<void> => {
  const manager = createWebSocketManager(async (token) =>
    token === 'valid-token'
      ? {
          subject: 'user-1',
          permissions: ['node:read'],
          traceId: 'trace-valid-token',
        }
      : null,
  );
  const missingTokenClient = createMockConnection('c1');

  expect(await manager.connect(missingTokenClient.connection, undefined)).toBe(false);
  expect(missingTokenClient.sent).toHaveLength(1);
  expect(JSON.parse(missingTokenClient.sent[0])).toMatchObject({
    type: 'ERROR',
    code: 'AUTH_REQUIRED',
  });

  const invalidTokenClient = createMockConnection('c2', 'bad-token');
  expect(await manager.connect(invalidTokenClient.connection, 'bad-token')).toBe(false);
  expect(JSON.parse(invalidTokenClient.sent[0])).toMatchObject({
    type: 'ERROR',
    code: 'AUTH_INVALID',
  });
});

test('websocket manager handles SUBSCRIBE, UNSUBSCRIBE and PING protocol', async (): Promise<void> => {
  const manager = createWebSocketManager(async (token) =>
    token === 'valid-token'
      ? {
          subject: 'user-1',
          permissions: ['node:read'],
          traceId: 'trace-valid-token',
        }
      : null,
  );
  const client = createMockConnection('c1', 'valid-token');

  expect(await manager.connect(client.connection, 'valid-token')).toBe(true);

  manager.handleMessage(client.connection, JSON.stringify({ type: 'SUBSCRIBE', topic: 'task.1.status' }));
  manager.handleMessage(client.connection, JSON.stringify({ type: 'PING' }));
  manager.handleMessage(client.connection, JSON.stringify({ type: 'UNSUBSCRIBE', topic: 'task.1.status' }));

  expect(JSON.parse(client.sent[0])).toMatchObject({
    type: 'ACK',
    action: 'CONNECTED',
  });
  expect(JSON.parse(client.sent[1])).toMatchObject({
    type: 'ACK',
    action: 'SUBSCRIBE',
    topic: 'task.1.status',
  });
  expect(JSON.parse(client.sent[2])).toMatchObject({
    type: 'ACK',
    action: 'PONG',
  });
  expect(JSON.parse(client.sent[3])).toMatchObject({
    type: 'ACK',
    action: 'UNSUBSCRIBE',
    topic: 'task.1.status',
  });

  expect(manager.broadcast('task.1.status', { state: 'running' })).toBe(0);
});

test('websocket manager broadcasts PUSH payload to subscribed topic only', async (): Promise<void> => {
  const manager = createWebSocketManager(allowToken);
  const taskClient = createMockConnection('task-client', 'valid-task');
  const statusClient = createMockConnection('status-client', 'valid-status');

  expect(await manager.connect(taskClient.connection, 'valid-task')).toBe(true);
  expect(await manager.connect(statusClient.connection, 'valid-status')).toBe(true);

  manager.handleMessage(taskClient.connection, JSON.stringify({ type: 'SUBSCRIBE', topic: 'task.1.status' }));
  manager.handleMessage(statusClient.connection, JSON.stringify({ type: 'SUBSCRIBE', topic: 'node.a.status' }));

  const delivered = manager.broadcast('task.1.status', { taskId: 't1', status: 'done' });
  expect(delivered).toBe(1);

  const taskPush = JSON.parse(taskClient.sent[2]);
  expect(taskPush).toMatchObject({
    type: 'PUSH',
    topic: 'task.1.status',
    payload: { taskId: 't1', status: 'done' },
    trace_id: 'trace-valid-task',
  });

  expect(statusClient.sent).toHaveLength(2);
});

test('websocket manager rejects disallowed topic subscription', async (): Promise<void> => {
  const manager = createWebSocketManager(allowToken);
  const client = createMockConnection('restricted', 'valid-restricted');

  expect(await manager.connect(client.connection, 'valid-restricted')).toBe(true);
  manager.handleMessage(client.connection, JSON.stringify({ type: 'SUBSCRIBE', topic: 'logs.node-1' }));

  expect(JSON.parse(client.sent[1])).toMatchObject({
    type: 'ERROR',
    code: 'INVALID_TOPIC',
  });
});

test('websocket manager rejects topic outside allowed topic contract', async (): Promise<void> => {
  const manager = createWebSocketManager(async (token) =>
    token === 'valid-token'
      ? {
          subject: 'user-1',
          permissions: ['node:read'],
          traceId: 'trace-valid-token',
          allowedTopics: ['task.1.status'],
        }
      : null,
  );

  const client = createMockConnection('c-allowed-topics', 'valid-token');

  expect(await manager.connect(client.connection, 'valid-token')).toBe(true);
  manager.handleMessage(client.connection, JSON.stringify({ type: 'SUBSCRIBE', topic: 'node.a.status' }));

  expect(JSON.parse(client.sent[1])).toMatchObject({
    type: 'ERROR',
    code: 'INVALID_TOPIC',
  });
});

test('websocket manager accepts sys.network.mode for sys:manage permission', async (): Promise<void> => {
  const manager = createWebSocketManager(async (token) =>
    token === 'valid-sys'
      ? {
          subject: 'ops-admin',
          permissions: ['sys:manage'],
          traceId: 'trace-valid-sys',
          allowedTopics: ['sys.network.mode'],
        }
      : null,
  );

  const client = createMockConnection('sys-client', 'valid-sys');

  expect(await manager.connect(client.connection, 'valid-sys')).toBe(true);
  manager.handleMessage(client.connection, JSON.stringify({ type: 'SUBSCRIBE', topic: 'sys.network.mode' }));

  expect(JSON.parse(client.sent[1])).toMatchObject({
    type: 'ACK',
    action: 'SUBSCRIBE',
    topic: 'sys.network.mode',
  });

  expect(manager.broadcast('sys.network.mode', { to: 'M-NET' })).toBe(1);
  expect(JSON.parse(client.sent[2])).toMatchObject({
    type: 'PUSH',
    topic: 'sys.network.mode',
    payload: { to: 'M-NET' },
  });
});

test('websocket manager applies stream profile throttling for high-frequency pushes', async (): Promise<void> => {
  const manager = createWebSocketManager(allowToken);
  const client = createMockConnection('c-stream-profile', 'valid-stream');

  expect(await manager.connect(client.connection, 'valid-stream')).toBe(true);
  manager.handleMessage(
    client.connection,
    JSON.stringify({
      type: 'SUBSCRIBE',
      topic: 'task.1.status',
      stream_profile: 'conserve',
    }),
  );

  expect(JSON.parse(client.sent[1])).toMatchObject({
    type: 'ACK',
    action: 'SUBSCRIBE',
    topic: 'task.1.status',
    stream_profile: 'conserve',
  });

  expect(manager.broadcast('task.1.status', { state: 'running' })).toBe(1);
  expect(manager.broadcast('task.1.status', { state: 'still-running' })).toBe(0);
});

test('ws handlers auto-subscribe topic from query when eden ws is enabled', async (): Promise<void> => {
  const manager = createWebSocketManager(allowToken);
  let handlers: WsHandlers | null = null;

  wsRoute(
    {
      ws: (_path: string, registeredHandlers: WsHandlers): void => {
        handlers = registeredHandlers;
      },
    } as unknown as Elysia,
    {
      manager,
      enableEdenSubscribe: true,
      wsPath: '/ws-test',
    },
  );

  expect(handlers).not.toBeNull();
  if (!handlers) {
    throw new Error('ws handlers should be registered');
  }
  const activeHandlers: WsHandlers = handlers;

  const client = createMockConnection('eden-enabled', 'valid-eden', {
    topic: 'task.eden.status',
  });
  activeHandlers.open(client.connection);
  await Bun.sleep(0);

  expect(JSON.parse(client.sent[0])).toMatchObject({
    type: 'ACK',
    action: 'CONNECTED',
  });
  expect(JSON.parse(client.sent[1])).toMatchObject({
    type: 'ACK',
    action: 'SUBSCRIBE',
    topic: 'task.eden.status',
  });

  expect(manager.broadcast('task.eden.status', { state: 'running' })).toBe(1);
  expect(JSON.parse(client.sent[2])).toMatchObject({
    type: 'PUSH',
    topic: 'task.eden.status',
  });
});

test('ws handlers do not auto-subscribe topic when eden ws is disabled', async (): Promise<void> => {
  const manager = createWebSocketManager(allowToken);
  let handlers: WsHandlers | null = null;

  wsRoute(
    {
      ws: (_path: string, registeredHandlers: WsHandlers): void => {
        handlers = registeredHandlers;
      },
    } as unknown as Elysia,
    {
      manager,
      enableEdenSubscribe: false,
      wsPath: '/ws-test',
    },
  );

  expect(handlers).not.toBeNull();
  if (!handlers) {
    throw new Error('ws handlers should be registered');
  }
  const activeHandlers: WsHandlers = handlers;

  const client = createMockConnection('eden-disabled', 'valid-eden', {
    topic: 'task.eden.status',
  });
  activeHandlers.open(client.connection);
  await Bun.sleep(0);

  expect(client.sent).toHaveLength(1);
  expect(JSON.parse(client.sent[0])).toMatchObject({
    type: 'ACK',
    action: 'CONNECTED',
  });
  expect(manager.broadcast('task.eden.status', { state: 'running' })).toBe(0);
});

test('wsRoute registers handlers and enforces token from query on open', async (): Promise<void> => {
  let registeredPath = '';
  let handlers: WsHandlers | null = null;

  const mockApp = {
    ws: (path: string, candidateHandlers: WsHandlers): void => {
      registeredPath = path;
      handlers = candidateHandlers;
    },
  };

  wsRoute(mockApp as unknown as Elysia, {
    wsPath: '/ws-test',
    validateToken: async (token) =>
      token === 'valid-token'
        ? {
            subject: 'user-1',
            permissions: ['node:read'],
            traceId: 'trace-valid-token',
          }
        : null,
  });

  expect(registeredPath).toBe('/ws-test');
  expect(handlers).not.toBeNull();
  if (!handlers) {
    throw new Error('ws handlers should be registered');
  }
  const activeHandlers: WsHandlers = handlers;

  const blockedClient = createMockConnection('blocked');
  activeHandlers.open(blockedClient.connection);
  await Bun.sleep(0);
  expect(blockedClient.closeCalls).toBe(1);
  expect(JSON.parse(blockedClient.sent[0])).toMatchObject({
    type: 'ERROR',
    code: 'AUTH_REQUIRED',
  });

  const acceptedClient = createMockConnection('accepted', 'valid-token');
  activeHandlers.open(acceptedClient.connection);
  await Bun.sleep(0);
  expect(acceptedClient.closeCalls).toBe(0);
  expect(JSON.parse(acceptedClient.sent[0])).toMatchObject({
    type: 'ACK',
    action: 'CONNECTED',
  });
});

test('default ws token validator accepts token signed by old secret during rotation', async (): Promise<void> => {
  process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET = 'new-sign-secret';
  process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS = 'new-sign-secret,old-verify-secret';

  const token = await new SignJWT({
    sub: 'user-ws',
    type: 'USER',
    permissions: ['node:read'],
    exp: Math.floor(Date.now() / 1000) + 120,
  })
    .setProtectedHeader({ alg: 'HS256' })
    .sign(new TextEncoder().encode('old-verify-secret'));

  const manager = createWebSocketManager();
  const client = createMockConnection('default-validator', token);

  expect(await manager.connect(client.connection, token)).toBe(true);
  expect(JSON.parse(client.sent[0])).toMatchObject({
    type: 'ACK',
    action: 'CONNECTED',
  });
});

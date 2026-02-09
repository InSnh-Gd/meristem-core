import { Elysia } from 'elysia';
import { test, expect } from 'bun:test';
import type { Collection, Db } from 'mongodb';
import { WIRE_CONTRACT_VERSION } from '@insnh-gd/meristem-shared';
import type { AuditEventInput, AuditLog } from '../services/audit';
import { NodeDocument } from '../db/collections';
import { createNode, generateHWID, joinRoute, recoverNode, Persona } from '../routes/join';

// 验证 generateHWID 针对固定 UUID / MAC 输出可预测的 64 字符哈希
test('generateHWID creates deterministic fingerprint', async (): Promise<void> => {
  const uuid = '00000000-0000-0000-0000-000000000000';
  const mac = '00:00:00:00:00:00';
  const hwid = await generateHWID(uuid, mac);

  expect(hwid.length).toBe(64);
  expect(hwid).toBe('c7b2c658b1239251f16e347ed880fc6cce3532556bde5fc9cb6b762947dc1660');
});

// 验证 createNode 能在目标集合中写入节点信息并返回完整文档
test('createNode persists node document with correct metadata', async (): Promise<void> => {
  const recordedNodes: NodeDocument[] = [];

  const nodeCollection = {
    insertOne: async (doc: NodeDocument): Promise<{ insertedId: string }> => {
      recordedNodes.push(doc);
      return { insertedId: 'mock-id' };
    },
  };

  const mockDb = {
    collection: (_name: string): Collection<NodeDocument> => {
      return nodeCollection as unknown as Collection<NodeDocument>;
    },
  };

  const hwid = 'test-hwid-create';
  const persona: Persona = 'AGENT';
  const result = await createNode(mockDb as Db, hwid, persona);

  expect(result.hwid).toBe(hwid);
  expect(result.persona).toBe(persona);
  expect(result.node_id.startsWith('node-')).toBe(true);
  expect(result.network.mode).toBe('DIRECT');
  expect(recordedNodes).toHaveLength(1);
  expect(recordedNodes[0]).toBe(result);
});

// 验证 recoverNode 能根据 HWID 找回已有节点，并在不存在时返回 null
test('recoverNode returns node when HWID exists', async (): Promise<void> => {
  const existingNode: NodeDocument = {
    node_id: 'node-recover',
    org_id: 'org-default',
    hwid: 'recover-hwid',
    hostname: 'host-recover',
    persona: 'GIG',
    role_flags: { is_relay: false, is_storage: false, is_compute: false },
    network: { virtual_ip: '10.25.1.5', mode: 'DIRECT', v: 0 },
    inventory: { cpu_model: 'test-cpu', cores: 4, ram_total: 16, os: 'linux', arch: 'x86_64' },
    status: {
      online: true,
      connection_status: 'online',
      last_seen: new Date('2026-01-01T00:00:00.000Z'),
      cpu_usage: 5,
      ram_free: 8,
      gpu_info: [],
    },
    created_at: new Date('2026-01-01T00:00:00.000Z'),
  };

  const nodeCollection = {
    findOne: async (query?: Record<string, unknown>): Promise<NodeDocument | null> => {
      const candidate = query?.hwid;
      if (typeof candidate === 'string' && candidate === existingNode.hwid) {
        return existingNode;
      }
      return null;
    },
  };

  const mockDb = {
    collection: (_name: string): Collection<NodeDocument> => {
      return nodeCollection as unknown as Collection<NodeDocument>;
    },
  };

  const recovered = await recoverNode(mockDb as Db, existingNode.hwid);
  expect(recovered).toBe(existingNode);

  const missing = await recoverNode(mockDb as Db, 'missing-hwid');
  expect(missing).toBeNull();
});

test('joinRoute logs audit event for new nodes', async (): Promise<void> => {
  const recordedNodes: NodeDocument[] = [];
  const newNodeHwid = 'n'.repeat(64);

  const nodeCollection = {
    findOne: async (_query?: Record<string, unknown>): Promise<NodeDocument | null> => null,
    insertOne: async (doc: NodeDocument): Promise<{ insertedId: string }> => {
      recordedNodes.push(doc);
      return { insertedId: doc.node_id };
    },
    updateOne: async (): Promise<{ modifiedCount: number }> => ({ modifiedCount: 1 }),
  };

  const mockDb = {
    collection: (_name: string): Collection<NodeDocument> => {
      return nodeCollection as unknown as Collection<NodeDocument>;
    },
  };

  const auditEvents: AuditEventInput[] = [];
  const auditLogger = async (_innerDb: Db, event: AuditEventInput): Promise<AuditLog> => {
    auditEvents.push(event);
    return {
      ...event,
      _sequence: 1,
      _hash: 'hash',
      _previous_hash: '',
    };
  };

  const app = new Elysia();
  joinRoute(app, mockDb as Db, auditLogger);

  const traceId = 'trace-new-node';
  const response = await app.handle(
    new Request('http://localhost/api/v1/join', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-trace-id': traceId,
      },
      body: JSON.stringify({
        hwid: newNodeHwid,
        hostname: 'new-node',
        persona: 'GIG',
      }),
    }),
  );

  const payload = await response.json();

  expect(response.status).toBe(200);
  expect(payload.success).toBe(true);
  expect(payload.data.status).toBe('new');
  expect(payload.data.node_id).toBeDefined();
  expect(recordedNodes).toHaveLength(1);
  expect(recordedNodes[0].hwid).toBe(newNodeHwid);
  expect(auditEvents).toHaveLength(1);
  expect(auditEvents[0]).toMatchObject({
    level: 'INFO',
    source: 'join',
    trace_id: traceId,
    content: 'Node joined',
    meta: { persona: 'GIG', status: 'new' },
  });
  expect(auditEvents[0].node_id).toBe(payload.data.node_id);

});

test('joinRoute logs audit event for existing nodes', async (): Promise<void> => {
  const existingNodeHwid = 'e'.repeat(64);
  const existingNode: NodeDocument = {
    node_id: 'node-existing',
    org_id: 'org-default',
    hwid: existingNodeHwid,
    hostname: 'old-host',
    persona: 'GIG',
    role_flags: { is_relay: false, is_storage: false, is_compute: false },
    network: { virtual_ip: '10.25.10.2', mode: 'DIRECT', v: 0 },
    inventory: { cpu_model: 'c', cores: 2, ram_total: 4, os: 'linux', arch: 'x86_64' },
    status: {
      online: true,
      connection_status: 'online',
      last_seen: new Date(),
      cpu_usage: 0,
      ram_free: 2,
      gpu_info: [],
    },
    created_at: new Date(),
  };

  const nodeCollection = {
    findOne: async (query?: Record<string, unknown>): Promise<NodeDocument | null> => {
      if (query?.hwid === existingNode.hwid) {
        return existingNode;
      }
      return null;
    },
    insertOne: async (): Promise<{ insertedId: string }> => ({ insertedId: 'should-not-be-used' }),
    updateOne: async (): Promise<{ modifiedCount: number }> => ({ modifiedCount: 1 }),
  };

  const mockDb = {
    collection: (_name: string): Collection<NodeDocument> => {
      return nodeCollection as unknown as Collection<NodeDocument>;
    },
  };

  const auditEvents: AuditEventInput[] = [];
  const auditLogger = async (_innerDb: Db, event: AuditEventInput): Promise<AuditLog> => {
    auditEvents.push(event);
    return {
      ...event,
      _sequence: 2,
      _hash: 'hash-existing',
      _previous_hash: '',
    };
  };

  const app = new Elysia();
  joinRoute(app, mockDb as Db, auditLogger);

  const traceId = 'trace-existing-node';
  const response = await app.handle(
    new Request('http://localhost/api/v1/join', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-trace-id': traceId,
      },
      body: JSON.stringify({
        hwid: existingNodeHwid,
        hostname: 'updated-host',
        persona: 'AGENT',
      }),
    }),
  );

  const payload = await response.json();

  expect(response.status).toBe(200);
  expect(payload.success).toBe(true);
  expect(payload.data.status).toBe('existing');
  expect(auditEvents).toHaveLength(1);
  expect(auditEvents[0]).toMatchObject({
    level: 'INFO',
    source: 'join',
    trace_id: traceId,
    content: 'Node joined',
  });
  expect(auditEvents[0].meta).toEqual({ persona: 'AGENT', status: 'existing', org_id: 'org-default' });
  expect(auditEvents[0].node_id).toBe(existingNode.node_id);

});

test('joinRoute rejects mismatched wire contract version header', async (): Promise<void> => {
  const nodeCollection = {
    findOne: async (): Promise<NodeDocument | null> => null,
    insertOne: async (): Promise<{ insertedId: string }> => ({ insertedId: 'n/a' }),
    updateOne: async (): Promise<{ modifiedCount: number }> => ({ modifiedCount: 0 }),
  };

  const mockDb = {
    collection: (_name: string): Collection<NodeDocument> =>
      nodeCollection as unknown as Collection<NodeDocument>,
  };

  const app = new Elysia();
  joinRoute(app, mockDb as Db);

  const response = await app.handle(
    new Request('http://localhost/api/v1/join', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-wire-contract-version': `${WIRE_CONTRACT_VERSION}-mismatch`,
      },
      body: JSON.stringify({
        hwid: 'f'.repeat(64),
        hostname: 'node-wire',
        persona: 'AGENT',
      }),
    }),
  );

  expect(response.status).toBe(400);
  await expect(response.json()).resolves.toEqual({
    success: false,
    error: 'WIRE_CONTRACT_VERSION_MISMATCH',
  });
});

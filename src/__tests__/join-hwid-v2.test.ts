import { Elysia } from 'elysia';
import { test, expect } from 'bun:test';
import type { Collection, Db } from 'mongodb';
import { type NodeDocument } from '../db/collections';
import { joinRoute } from '../routes/join';
import type { AuditEventInput, AuditLog } from '../services/audit';

type HardwareProfileForTest = {
  cpu?: {
    model: string;
    cores: number;
    threads?: number;
  };
  memory?: {
    total: number;
    available?: number;
    type?: string;
  };
  storage?: Array<{
    type?: string;
    size?: number;
    total?: number;
    available?: number;
  }>;
  gpu?: Array<{
    model: string;
    vram?: number;
    memory?: number;
  }>;
  os?: string;
  arch?: 'x86_64' | 'arm64' | 'unknown';
};

type UpdateOperationRecord = {
  filter: Record<string, unknown>;
  update: Record<string, unknown>;
};

type NodeCollectionMock = {
  findOne: (query?: Record<string, unknown>) => Promise<NodeDocument | null>;
  insertOne: (doc: NodeDocument) => Promise<{ insertedId: string }>;
  updateOne: (
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
  ) => Promise<{ modifiedCount: number }>;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const canonicalizeForHash = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map((item) => canonicalizeForHash(item));
  }

  if (isRecord(value)) {
    const next: Record<string, unknown> = {};
    const sortedKeys = Object.keys(value).sort();
    for (const key of sortedKeys) {
      const candidate = value[key];
      if (candidate !== undefined) {
        next[key] = canonicalizeForHash(candidate);
      }
    }
    return next;
  }

  return value;
};

const createHardwareProfileHash = async (profile: HardwareProfileForTest): Promise<string> => {
  const canonicalProfile = canonicalizeForHash(profile);
  const payload = JSON.stringify(canonicalProfile);
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(payload));
  return Array.from(new Uint8Array(digest))
    .map((byteValue) => byteValue.toString(16).padStart(2, '0'))
    .join('');
};

const createBaselineProfile = (): HardwareProfileForTest => ({
  cpu: {
    model: 'AMD Ryzen 7',
    cores: 8,
    threads: 16,
  },
  memory: {
    total: 17179869184,
    available: 8589934592,
    type: 'DDR5',
  },
  os: 'linux',
  arch: 'x86_64',
});

const createDbMock = (nodeCollection: NodeCollectionMock): Db => {
  const dbMock = {
    collection: (name: string): Collection<NodeDocument> => {
      if (name === 'nodes') {
        return nodeCollection as unknown as Collection<NodeDocument>;
      }

      const noopCollection = {
        findOne: async (): Promise<null> => null,
        insertOne: async (): Promise<{ insertedId: string }> => ({ insertedId: 'noop' }),
        updateOne: async (): Promise<{ modifiedCount: number }> => ({ modifiedCount: 0 }),
      };
      return noopCollection as unknown as Collection<NodeDocument>;
    },
  };
  return dbMock as unknown as Db;
};

const createAuditLogger = (events: AuditEventInput[]) => {
  return async (_db: Db, event: AuditEventInput): Promise<AuditLog> => {
    events.push(event);
    return {
      ...event,
      _sequence: events.length,
      _hash: `hash-${events.length}`,
      _previous_hash: events.length > 1 ? `hash-${events.length - 1}` : '',
    };
  };
};

test('joinRoute initializes hardware profile baseline for new nodes', async (): Promise<void> => {
  const recordedNodes: NodeDocument[] = [];
  const updates: UpdateOperationRecord[] = [];
  const baselineProfile = createBaselineProfile();
  const baselineHash = await createHardwareProfileHash(baselineProfile);

  const nodeCollection: NodeCollectionMock = {
    findOne: async (): Promise<NodeDocument | null> => null,
    insertOne: async (doc: NodeDocument): Promise<{ insertedId: string }> => {
      recordedNodes.push(doc);
      return { insertedId: doc.node_id };
    },
    updateOne: async (
      filter: Record<string, unknown>,
      update: Record<string, unknown>,
    ): Promise<{ modifiedCount: number }> => {
      updates.push({ filter, update });
      return { modifiedCount: 1 };
    },
  };

  (global as { db?: Db }).db = createDbMock(nodeCollection);
  const auditEvents: AuditEventInput[] = [];
  const app = new Elysia();
  joinRoute(app, createAuditLogger(auditEvents));

  const response = await app.handle(
    new Request('http://localhost/api/v1/join', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        hwid: 'a'.repeat(64),
        hostname: 'new-hwid-v2-node',
        persona: 'GIG',
        hardware_profile: baselineProfile,
        hardware_profile_hash: baselineHash,
      }),
    }),
  );
  const payload = await response.json();

  expect(response.status).toBe(200);
  expect(payload.success).toBe(true);
  expect(payload.data.status).toBe('new');
  expect(recordedNodes).toHaveLength(1);

  const createdNode = recordedNodes[0] as unknown as Record<string, unknown>;
  expect(createdNode.hardware_profile).toEqual(baselineProfile);
  expect(createdNode.hardware_profile_hash).toBe(baselineHash);

  const driftInfo = createdNode.hardware_profile_drift as Record<string, unknown> | undefined;
  expect(driftInfo?.detected).toBe(false);
  expect(driftInfo?.baseline_hash).toBe(baselineHash);
  expect(updates).toHaveLength(0);
  expect(auditEvents).toHaveLength(1);

  delete (global as { db?: Db }).db;
});

test('joinRoute restores existing node when hardware profile hash matches baseline', async (): Promise<void> => {
  const updates: UpdateOperationRecord[] = [];
  const baselineProfile = createBaselineProfile();
  const baselineHash = await createHardwareProfileHash(baselineProfile);

  const existingNode: NodeDocument & {
    hardware_profile_hash?: string;
  } = {
    node_id: 'node-hash-stable',
    hwid: 'b'.repeat(64),
    hostname: 'old-node',
    persona: 'GIG',
    role_flags: { is_relay: false, is_storage: false, is_compute: true },
    network: { virtual_ip: '10.25.10.20', mode: 'DIRECT', v: 3 },
    inventory: { cpu_model: 'x', cores: 8, ram_total: 16, os: 'linux', arch: 'x86_64' },
    status: {
      online: false,
      connection_status: 'offline',
      last_seen: new Date('2026-01-01T00:00:00.000Z'),
      cpu_usage: 0,
      ram_free: 0,
      gpu_info: [],
    },
    created_at: new Date('2026-01-01T00:00:00.000Z'),
    hardware_profile_hash: baselineHash,
  };

  const nodeCollection: NodeCollectionMock = {
    findOne: async (): Promise<NodeDocument | null> => existingNode,
    insertOne: async (): Promise<{ insertedId: string }> => ({ insertedId: 'unused' }),
    updateOne: async (
      filter: Record<string, unknown>,
      update: Record<string, unknown>,
    ): Promise<{ modifiedCount: number }> => {
      updates.push({ filter, update });
      return { modifiedCount: 1 };
    },
  };

  (global as { db?: Db }).db = createDbMock(nodeCollection);
  const app = new Elysia();
  joinRoute(app, createAuditLogger([]));

  const response = await app.handle(
    new Request('http://localhost/api/v1/join', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        hwid: existingNode.hwid,
        hostname: 'restored-node',
        persona: 'AGENT',
        hardware_profile: baselineProfile,
        hardware_profile_hash: baselineHash,
      }),
    }),
  );
  const payload = await response.json();

  expect(response.status).toBe(200);
  expect(payload.success).toBe(true);
  expect(payload.data.status).toBe('existing');
  expect(updates).toHaveLength(1);

  const updateSet = updates[0]?.update.$set as Record<string, unknown> | undefined;
  expect(updateSet?.['status.connection_status']).toBe('online');
  expect(updateSet?.['status.online']).toBe(true);
  expect(updateSet?.hardware_profile_hash).toBe(baselineHash);
  const driftInfo = updateSet?.hardware_profile_drift as Record<string, unknown> | undefined;
  expect(driftInfo?.detected).toBe(false);

  delete (global as { db?: Db }).db;
});

test('joinRoute blocks recovery when hardware profile hash drifts', async (): Promise<void> => {
  const updates: UpdateOperationRecord[] = [];
  const baselineProfile = createBaselineProfile();
  const baselineHash = await createHardwareProfileHash(baselineProfile);
  const driftProfile: HardwareProfileForTest = {
    ...baselineProfile,
    cpu: {
      model: 'AMD Ryzen 9',
      cores: 16,
      threads: 32,
    },
  };
  const driftHash = await createHardwareProfileHash(driftProfile);

  const existingNode: NodeDocument & {
    hardware_profile_hash?: string;
  } = {
    node_id: 'node-hash-drift',
    hwid: 'c'.repeat(64),
    hostname: 'drift-node',
    persona: 'GIG',
    role_flags: { is_relay: false, is_storage: false, is_compute: true },
    network: { virtual_ip: '10.25.10.21', mode: 'DIRECT', v: 1 },
    inventory: { cpu_model: 'x', cores: 8, ram_total: 16, os: 'linux', arch: 'x86_64' },
    status: {
      online: true,
      connection_status: 'online',
      last_seen: new Date('2026-01-01T00:00:00.000Z'),
      cpu_usage: 0,
      ram_free: 0,
      gpu_info: [],
    },
    created_at: new Date('2026-01-01T00:00:00.000Z'),
    hardware_profile_hash: baselineHash,
  };

  const nodeCollection: NodeCollectionMock = {
    findOne: async (): Promise<NodeDocument | null> => existingNode,
    insertOne: async (): Promise<{ insertedId: string }> => ({ insertedId: 'unused' }),
    updateOne: async (
      filter: Record<string, unknown>,
      update: Record<string, unknown>,
    ): Promise<{ modifiedCount: number }> => {
      updates.push({ filter, update });
      return { modifiedCount: 1 };
    },
  };

  (global as { db?: Db }).db = createDbMock(nodeCollection);
  const app = new Elysia();
  joinRoute(app, createAuditLogger([]));

  const response = await app.handle(
    new Request('http://localhost/api/v1/join', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        hwid: existingNode.hwid,
        hostname: 'drifted-node',
        persona: 'AGENT',
        hardware_profile: driftProfile,
        hardware_profile_hash: driftHash,
      }),
    }),
  );
  const payload = await response.json();

  expect(response.status).toBe(200);
  expect(payload.success).toBe(true);
  expect(payload.data.status).toBe('pending_approval');
  expect(updates).toHaveLength(1);

  const updateSet = updates[0]?.update.$set as Record<string, unknown> | undefined;
  expect(updateSet?.['status.connection_status']).toBe('pending_approval');
  expect(updateSet?.['status.online']).toBe(false);
  const driftInfo = updateSet?.hardware_profile_drift as Record<string, unknown> | undefined;
  expect(driftInfo?.detected).toBe(true);
  expect(driftInfo?.baseline_hash).toBe(baselineHash);
  expect(driftInfo?.incoming_hash).toBe(driftHash);

  delete (global as { db?: Db }).db;
});

test('joinRoute writes drift audit event when hardware profile hash drifts', async (): Promise<void> => {
  const baselineProfile = createBaselineProfile();
  const baselineHash = await createHardwareProfileHash(baselineProfile);
  const driftProfile: HardwareProfileForTest = {
    ...baselineProfile,
    memory: {
      total: 34359738368,
      available: 17179869184,
      type: 'DDR5',
    },
  };
  const driftHash = await createHardwareProfileHash(driftProfile);

  const existingNode: NodeDocument & {
    hardware_profile_hash?: string;
  } = {
    node_id: 'node-hash-drift-audit',
    hwid: 'd'.repeat(64),
    hostname: 'audit-drift-node',
    persona: 'GIG',
    role_flags: { is_relay: false, is_storage: false, is_compute: true },
    network: { virtual_ip: '10.25.10.22', mode: 'DIRECT', v: 1 },
    inventory: { cpu_model: 'x', cores: 8, ram_total: 16, os: 'linux', arch: 'x86_64' },
    status: {
      online: true,
      connection_status: 'online',
      last_seen: new Date('2026-01-01T00:00:00.000Z'),
      cpu_usage: 0,
      ram_free: 0,
      gpu_info: [],
    },
    created_at: new Date('2026-01-01T00:00:00.000Z'),
    hardware_profile_hash: baselineHash,
  };

  const nodeCollection: NodeCollectionMock = {
    findOne: async (): Promise<NodeDocument | null> => existingNode,
    insertOne: async (): Promise<{ insertedId: string }> => ({ insertedId: 'unused' }),
    updateOne: async (): Promise<{ modifiedCount: number }> => ({ modifiedCount: 1 }),
  };

  (global as { db?: Db }).db = createDbMock(nodeCollection);
  const auditEvents: AuditEventInput[] = [];
  const app = new Elysia();
  joinRoute(app, createAuditLogger(auditEvents));

  const response = await app.handle(
    new Request('http://localhost/api/v1/join', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-trace-id': 'trace-drift-audit',
      },
      body: JSON.stringify({
        hwid: existingNode.hwid,
        hostname: 'drifted-node',
        persona: 'AGENT',
        hardware_profile: driftProfile,
        hardware_profile_hash: driftHash,
      }),
    }),
  );

  expect(response.status).toBe(200);
  expect(auditEvents).toHaveLength(1);
  expect(auditEvents[0]).toMatchObject({
    level: 'WARN',
    source: 'join',
    trace_id: 'trace-drift-audit',
    content: 'Node join blocked by hardware profile drift',
  });

  const auditMeta = auditEvents[0]?.meta as Record<string, unknown>;
  expect(auditMeta.status).toBe('pending_approval');
  expect(auditMeta.drift_detected).toBe(true);
  expect(auditMeta.baseline_hash).toBe(baselineHash);
  expect(auditMeta.incoming_hash).toBe(driftHash);

  delete (global as { db?: Db }).db;
});

test('joinRoute rejects mismatched hardware_profile_hash when profile is provided', async (): Promise<void> => {
  const updates: UpdateOperationRecord[] = [];
  const baselineProfile = createBaselineProfile();
  const realHash = await createHardwareProfileHash(baselineProfile);
  const fakeHash = realHash.replace(/^./, realHash[0] === 'a' ? 'b' : 'a');

  const nodeCollection: NodeCollectionMock = {
    findOne: async (): Promise<NodeDocument | null> => null,
    insertOne: async (): Promise<{ insertedId: string }> => ({ insertedId: 'unused' }),
    updateOne: async (
      filter: Record<string, unknown>,
      update: Record<string, unknown>,
    ): Promise<{ modifiedCount: number }> => {
      updates.push({ filter, update });
      return { modifiedCount: 1 };
    },
  };

  (global as { db?: Db }).db = createDbMock(nodeCollection);
  const app = new Elysia();
  joinRoute(app, createAuditLogger([]));

  const response = await app.handle(
    new Request('http://localhost/api/v1/join', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        hwid: 'e'.repeat(64),
        hostname: 'hash-mismatch-node',
        persona: 'GIG',
        hardware_profile: baselineProfile,
        hardware_profile_hash: fakeHash,
      }),
    }),
  );
  const payload = await response.json();

  expect(response.status).toBe(400);
  expect(payload.success).toBe(false);
  expect(payload.error).toBe('HARDWARE_PROFILE_HASH_MISMATCH');
  expect(updates).toHaveLength(0);

  delete (global as { db?: Db }).db;
});

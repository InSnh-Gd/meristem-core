import type { Collection, Db, IndexDescription } from 'mongodb';
import type { TraceContext } from '../utils/trace-context';
import { createLogger } from '../utils/logger';
import {
  INVITATIONS_COLLECTION,
  NODES_COLLECTION,
  ORGS_COLLECTION,
  PLUGINS_COLLECTION,
  ROLES_COLLECTION,
  TASKS_COLLECTION,
  USERS_COLLECTION,
} from './collections';

export type IndexSpec = IndexDescription;

type CollectionIndexPlan = {
  collection: string;
  specs: readonly IndexSpec[];
};

const INDEX_PLANS: readonly CollectionIndexPlan[] = [
  {
    collection: USERS_COLLECTION,
    specs: [
      { key: { user_id: 1 }, name: 'uniq_user_id', unique: true },
      { key: { username: 1 }, name: 'uniq_username', unique: true },
      { key: { created_at: 1, user_id: 1 }, name: 'idx_users_created_user' },
      { key: { org_id: 1, created_at: 1, user_id: 1 }, name: 'idx_users_org_created_user' },
    ],
  },
  {
    collection: ROLES_COLLECTION,
    specs: [
      { key: { role_id: 1 }, name: 'uniq_role_id', unique: true },
      { key: { org_id: 1, name: 1 }, name: 'uniq_roles_org_name', unique: true },
      { key: { created_at: 1, role_id: 1 }, name: 'idx_roles_created_role' },
      { key: { org_id: 1, created_at: 1, role_id: 1 }, name: 'idx_roles_org_created_role' },
    ],
  },
  {
    collection: TASKS_COLLECTION,
    specs: [
      { key: { task_id: 1 }, name: 'uniq_task_id', unique: true },
      { key: { created_at: 1, task_id: 1 }, name: 'idx_tasks_created_task' },
      { key: { org_id: 1, created_at: 1, task_id: 1 }, name: 'idx_tasks_org_created_task' },
      {
        key: { 'status.type': 1, target_node_id: 1, created_at: 1, task_id: 1 },
        name: 'idx_tasks_status_target_created_task',
      },
      { key: { owner_id: 1, created_at: 1, task_id: 1 }, name: 'idx_tasks_owner_created_task' },
      { key: { trace_id: 1 }, name: 'idx_tasks_trace' },
    ],
  },
  {
    collection: NODES_COLLECTION,
    specs: [
      { key: { node_id: 1 }, name: 'uniq_node_id', unique: true },
      { key: { hwid: 1 }, name: 'uniq_node_hwid', unique: true },
      { key: { org_id: 1, persona: 1 }, name: 'idx_nodes_org_persona' },
      { key: { 'status.online': 1, 'status.last_seen': 1 }, name: 'idx_nodes_online_last_seen' },
    ],
  },
  {
    collection: ORGS_COLLECTION,
    specs: [
      { key: { org_id: 1 }, name: 'uniq_org_id', unique: true },
      { key: { slug: 1 }, name: 'uniq_org_slug', unique: true },
    ],
  },
  {
    collection: INVITATIONS_COLLECTION,
    specs: [
      { key: { invitation_id: 1 }, name: 'uniq_invitation_id', unique: true },
      { key: { invitation_token: 1 }, name: 'uniq_invitation_token', unique: true },
      { key: { status: 1, expires_at: 1 }, name: 'idx_invitations_status_expire' },
      { key: { org_id: 1, username: 1 }, name: 'idx_invitations_org_username' },
    ],
  },
  {
    collection: PLUGINS_COLLECTION,
    specs: [
      { key: { plugin_id: 1 }, name: 'uniq_plugin_id', unique: true },
      { key: { name: 1, version: 1 }, name: 'uniq_plugin_name_version', unique: true },
      { key: { status: 1 }, name: 'idx_plugins_status' },
    ],
  },
  {
    collection: 'audit_logs',
    specs: [
      { key: { _sequence: 1 }, name: 'uniq_audit_sequence', unique: true },
      { key: { ts: -1 }, name: 'idx_audit_ts_desc' },
      { key: { trace_id: 1, _sequence: 1 }, name: 'idx_audit_trace_sequence' },
      { key: { source: 1, ts: -1 }, name: 'idx_audit_source_ts' },
      { key: { level: 1, ts: -1 }, name: 'idx_audit_level_ts' },
      { key: { 'meta.actor': 1, ts: -1 }, name: 'idx_audit_actor_ts' },
    ],
  },
];

const createCollectionIndexes = async (
  collection: Collection,
  specs: readonly IndexSpec[],
): Promise<void> => {
  if (specs.length === 0) {
    return;
  }
  await collection.createIndexes([...specs]);
};

type IndexListCapableCollection = Collection & {
  listIndexes?: () => {
    toArray: () => Promise<Array<{ name?: string }>>;
  };
};

const extractExpectedIndexNames = (specs: readonly IndexSpec[]): string[] =>
  specs
    .map((spec) => (typeof spec.name === 'string' ? spec.name : undefined))
    .filter((name): name is string => typeof name === 'string' && name.length > 0);

const resolveMissingIndexNames = async (
  collection: Collection,
  expectedNames: readonly string[],
): Promise<string[]> => {
  const collectionWithListIndexes = collection as IndexListCapableCollection;
  if (typeof collectionWithListIndexes.listIndexes !== 'function') {
    return [];
  }

  try {
    const existingIndexes = await collectionWithListIndexes.listIndexes().toArray();
    const existingNames = new Set(
      existingIndexes
        .map((item) => item.name)
        .filter((name): name is string => typeof name === 'string' && name.length > 0),
    );

    return expectedNames.filter((name) => !existingNames.has(name));
  } catch {
    return [];
  }
};

export const ensureDbIndexes = async (
  db: Db,
  traceContext: TraceContext,
): Promise<void> => {
  const logger = createLogger(traceContext);
  const startedAt = Date.now();

  for (const plan of INDEX_PLANS) {
    const collection = db.collection(plan.collection);
    await createCollectionIndexes(collection, plan.specs);

    const expectedNames = extractExpectedIndexNames(plan.specs);
    const missingIndexNames = await resolveMissingIndexNames(collection, expectedNames);
    if (missingIndexNames.length > 0) {
      logger.warn('[DB] 索引校验缺失', {
        collection: plan.collection,
        missing_indexes: missingIndexNames,
      });
    }
  }

  const elapsedMs = Date.now() - startedAt;
  logger.info(`[DB] 索引初始化完成，共 ${INDEX_PLANS.length} 个集合，耗时 ${elapsedMs}ms`);
};

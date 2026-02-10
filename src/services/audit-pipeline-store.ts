import type { Collection, Db, Document } from 'mongodb';
import { AUDIT_COLLECTION, AUDIT_STATE_COLLECTION } from './audit';

export const AUDIT_INTENTS_COLLECTION = 'audit_intents';
export const AUDIT_PARTITION_STATE_COLLECTION = 'audit_partition_state';
export const AUDIT_GLOBAL_ANCHOR_COLLECTION = 'audit_global_anchor';
export const AUDIT_FAILURES_COLLECTION = 'audit_failures';

export const getIntentsCollection = <T extends Document>(db: Db): Collection<T> =>
  db.collection<T>(AUDIT_INTENTS_COLLECTION);

export const getLogsCollection = <T extends Document>(db: Db): Collection<T> =>
  db.collection<T>(AUDIT_COLLECTION);

export const getStateCollection = <T extends Document>(db: Db): Collection<T> =>
  db.collection<T>(AUDIT_STATE_COLLECTION);

export const getPartitionStateCollection = <T extends Document>(db: Db): Collection<T> =>
  db.collection<T>(AUDIT_PARTITION_STATE_COLLECTION);

export const getAnchorCollection = <T extends Document>(db: Db): Collection<T> =>
  db.collection<T>(AUDIT_GLOBAL_ANCHOR_COLLECTION);

export const getFailureCollection = <T extends Document>(db: Db): Collection<T> =>
  db.collection<T>(AUDIT_FAILURES_COLLECTION);

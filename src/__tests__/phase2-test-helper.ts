import type { Collection, Db, Document } from 'mongodb';

import type {
  InvitationDocument,
  NodeDocument,
  OrgDocument,
  RoleDocument,
  TaskDocument,
  UserDocument,
} from '../db/collections';
import {
  INVITATIONS_COLLECTION,
  NODES_COLLECTION,
  ORGS_COLLECTION,
  ROLES_COLLECTION,
  TASKS_COLLECTION,
  USERS_COLLECTION,
} from '../db/collections';
import { AUDIT_COLLECTION, type AuditLog } from '../services/audit';

type InMemoryDoc =
  | UserDocument
  | RoleDocument
  | OrgDocument
  | InvitationDocument
  | TaskDocument
  | NodeDocument
  | AuditLog;

type CollectionState = {
  users: UserDocument[];
  roles: RoleDocument[];
  orgs: OrgDocument[];
  invitations: InvitationDocument[];
  tasks: TaskDocument[];
  nodes: NodeDocument[];
  audits: AuditLog[];
};

type Cursor<TDoc extends InMemoryDoc> = {
  sort: (spec: Record<string, 1 | -1>) => Cursor<TDoc>;
  skip: (count: number) => Cursor<TDoc>;
  limit: (count: number) => Cursor<TDoc>;
  maxTimeMS: (value: number) => Cursor<TDoc>;
  toArray: () => Promise<TDoc[]>;
};

type UpdatePayload = {
  $set?: Record<string, unknown>;
  $unset?: Record<string, ''>;
  $inc?: Record<string, number>;
};

const asRecord = (value: unknown): Record<string, unknown> =>
  value as Record<string, unknown>;

const getValueByPath = (doc: Record<string, unknown>, path: string): unknown => {
  const segments = path.split('.');
  let current: unknown = doc;
  for (const segment of segments) {
    if (!current || typeof current !== 'object' || Array.isArray(current)) {
      return undefined;
    }
    current = asRecord(current)[segment];
  }
  return current;
};

const setValueByPath = (doc: Record<string, unknown>, path: string, value: unknown): void => {
  const segments = path.split('.');
  let current = doc;
  for (let i = 0; i < segments.length - 1; i += 1) {
    const segment = segments[i];
    const candidate = current[segment];
    if (!candidate || typeof candidate !== 'object' || Array.isArray(candidate)) {
      current[segment] = {};
    }
    current = asRecord(current[segment]);
  }
  current[segments[segments.length - 1]] = value;
};

const unsetValueByPath = (doc: Record<string, unknown>, path: string): void => {
  const segments = path.split('.');
  let current = doc;
  for (let i = 0; i < segments.length - 1; i += 1) {
    const segment = segments[i];
    const candidate = current[segment];
    if (!candidate || typeof candidate !== 'object' || Array.isArray(candidate)) {
      return;
    }
    current = asRecord(candidate);
  }
  delete current[segments[segments.length - 1]];
};

const toComparableValue = (value: unknown): unknown => {
  if (value instanceof Date) {
    return value.getTime();
  }
  return value;
};

const compareComparableValues = (
  left: unknown,
  right: unknown,
): number | null => {
  const normalizedLeft = toComparableValue(left);
  const normalizedRight = toComparableValue(right);

  if (typeof normalizedLeft === 'number' && typeof normalizedRight === 'number') {
    return normalizedLeft - normalizedRight;
  }
  if (typeof normalizedLeft === 'string' && typeof normalizedRight === 'string') {
    return normalizedLeft.localeCompare(normalizedRight);
  }
  return null;
};

const matchOperatorFilter = (
  actual: unknown,
  expectedRecord: Record<string, unknown>,
): boolean => {
  if ('$in' in expectedRecord && Array.isArray(expectedRecord.$in)) {
    return (expectedRecord.$in as unknown[]).some(
      (candidate) => toComparableValue(candidate) === toComparableValue(actual),
    );
  }

  if ('$ne' in expectedRecord) {
    return toComparableValue(actual) !== toComparableValue(expectedRecord.$ne);
  }

  if ('$gt' in expectedRecord) {
    const compareResult = compareComparableValues(actual, expectedRecord.$gt);
    if (compareResult === null) {
      return false;
    }
    return compareResult > 0;
  }

  if ('$gte' in expectedRecord) {
    const compareResult = compareComparableValues(actual, expectedRecord.$gte);
    if (compareResult === null) {
      return false;
    }
    return compareResult >= 0;
  }

  if ('$lt' in expectedRecord) {
    const compareResult = compareComparableValues(actual, expectedRecord.$lt);
    if (compareResult === null) {
      return false;
    }
    return compareResult < 0;
  }

  if ('$lte' in expectedRecord) {
    const compareResult = compareComparableValues(actual, expectedRecord.$lte);
    if (compareResult === null) {
      return false;
    }
    return compareResult <= 0;
  }

  return false;
};

const matchesFilter = (doc: Record<string, unknown>, filter?: Record<string, unknown>): boolean => {
  if (!filter) {
    return true;
  }

  if ('$and' in filter && Array.isArray(filter.$and)) {
    return (filter.$and as unknown[]).every((item) =>
      matchesFilter(doc, item as Record<string, unknown>),
    );
  }

  if ('$or' in filter && Array.isArray(filter.$or)) {
    return (filter.$or as unknown[]).some((item) =>
      matchesFilter(doc, item as Record<string, unknown>),
    );
  }

  return Object.entries(filter).every(([key, expected]) => {
    const actual = getValueByPath(doc, key);
    if (expected && typeof expected === 'object' && !Array.isArray(expected)) {
      const expectedRecord = asRecord(expected);
      if (
        '$in' in expectedRecord ||
        '$ne' in expectedRecord ||
        '$gt' in expectedRecord ||
        '$gte' in expectedRecord ||
        '$lt' in expectedRecord ||
        '$lte' in expectedRecord
      ) {
        return matchOperatorFilter(actual, expectedRecord);
      }
    }
    return toComparableValue(actual) === toComparableValue(expected);
  });
};

const createCursor = <TDoc extends InMemoryDoc>(items: TDoc[]): Cursor<TDoc> => {
  let rows = [...items];
  const cursor: Cursor<TDoc> = {
    sort: (spec) => {
      const entries = Object.entries(spec);
      rows = [...rows].sort((a, b) => {
        for (const [path, direction] of entries) {
          const aValue = getValueByPath(asRecord(a), path);
          const bValue = getValueByPath(asRecord(b), path);
          if (aValue === bValue) {
            continue;
          }
          if (aValue === undefined || aValue === null) {
            return 1;
          }
          if (bValue === undefined || bValue === null) {
            return -1;
          }
          if (aValue < bValue) {
            return direction === 1 ? -1 : 1;
          }
          return direction === 1 ? 1 : -1;
        }
        return 0;
      });
      return cursor;
    },
    skip: (count) => {
      rows = rows.slice(count);
      return cursor;
    },
    limit: (count) => {
      rows = rows.slice(0, count);
      return cursor;
    },
    maxTimeMS: () => cursor,
    toArray: async () => [...rows],
  };
  return cursor;
};

const cloneDoc = <TDoc extends InMemoryDoc>(doc: TDoc): TDoc => ({ ...doc });

const applyUpdate = <TDoc extends InMemoryDoc>(doc: TDoc, update: UpdatePayload): TDoc => {
  const next = cloneDoc(doc);
  if (update.$set) {
    for (const [path, value] of Object.entries(update.$set)) {
      setValueByPath(asRecord(next), path, value);
    }
  }
  if (update.$inc) {
    for (const [path, incValue] of Object.entries(update.$inc)) {
      const current = getValueByPath(asRecord(next), path);
      const currentNumber = typeof current === 'number' ? current : 0;
      setValueByPath(asRecord(next), path, currentNumber + incValue);
    }
  }
  if (update.$unset) {
    for (const path of Object.keys(update.$unset)) {
      unsetValueByPath(asRecord(next), path);
    }
  }
  return next;
};

type MutableCollection<TDoc extends InMemoryDoc> = {
  data: TDoc[];
};

const createCollection = <TDoc extends InMemoryDoc>(
  state: MutableCollection<TDoc>,
): Collection<TDoc> => {
  const collection = {
    countDocuments: async (filter?: Record<string, unknown>): Promise<number> =>
      state.data.filter((doc) => matchesFilter(asRecord(doc), filter)).length,
    findOne: async (filter?: Record<string, unknown>, options?: Record<string, unknown>): Promise<TDoc | null> => {
      let rows = state.data.filter((doc) => matchesFilter(asRecord(doc), filter));
      if (options?.sort && typeof options.sort === 'object') {
        rows = await createCursor(rows).sort(options.sort as Record<string, 1 | -1>).toArray();
      }
      return rows[0] ?? null;
    },
    find: (filter?: Record<string, unknown>): Cursor<TDoc> =>
      createCursor(state.data.filter((doc) => matchesFilter(asRecord(doc), filter))),
    insertOne: async (doc: TDoc): Promise<{ insertedId: string }> => {
      state.data.push(doc);
      const id =
        (getValueByPath(asRecord(doc), 'user_id') as string | undefined) ??
        (getValueByPath(asRecord(doc), 'role_id') as string | undefined) ??
        (getValueByPath(asRecord(doc), 'org_id') as string | undefined) ??
        (getValueByPath(asRecord(doc), 'invitation_id') as string | undefined) ??
        (getValueByPath(asRecord(doc), 'task_id') as string | undefined) ??
        (getValueByPath(asRecord(doc), 'node_id') as string | undefined) ??
        (getValueByPath(asRecord(doc), 'id') as string | undefined) ??
        'inserted';
      return { insertedId: id };
    },
    updateOne: async (
      filter: Record<string, unknown>,
      update: UpdatePayload,
    ): Promise<{ modifiedCount: number }> => {
      const index = state.data.findIndex((doc) => matchesFilter(asRecord(doc), filter));
      if (index < 0) {
        return { modifiedCount: 0 };
      }
      state.data[index] = applyUpdate(state.data[index], update);
      return { modifiedCount: 1 };
    },
    deleteOne: async (filter: Record<string, unknown>): Promise<{ deletedCount: number }> => {
      const index = state.data.findIndex((doc) => matchesFilter(asRecord(doc), filter));
      if (index < 0) {
        return { deletedCount: 0 };
      }
      state.data.splice(index, 1);
      return { deletedCount: 1 };
    },
    findOneAndUpdate: async (
      filter: Record<string, unknown>,
      update: UpdatePayload,
      options?: Record<string, unknown>,
    ): Promise<TDoc | null> => {
      const index = state.data.findIndex((doc) => matchesFilter(asRecord(doc), filter));
      if (index < 0) {
        return null;
      }
      const next = applyUpdate(state.data[index], update);
      state.data[index] = next;
      if (options?.returnDocument === 'before') {
        return state.data[index];
      }
      return next;
    },
  };

  return collection as unknown as Collection<TDoc>;
};

export type InMemoryDbState = CollectionState;

export const createInMemoryDbState = (): InMemoryDbState => ({
  users: [],
  roles: [],
  orgs: [],
  invitations: [],
  tasks: [],
  nodes: [],
  audits: [],
});

export const createInMemoryDb = (state: InMemoryDbState): Db => {
  const db = {
    collection: <TSchema extends Document>(name: string): Collection<TSchema> => {
      if (name === USERS_COLLECTION) {
        return createCollection<UserDocument>({ data: state.users }) as unknown as Collection<TSchema>;
      }
      if (name === ROLES_COLLECTION) {
        return createCollection<RoleDocument>({ data: state.roles }) as unknown as Collection<TSchema>;
      }
      if (name === ORGS_COLLECTION) {
        return createCollection<OrgDocument>({ data: state.orgs }) as unknown as Collection<TSchema>;
      }
      if (name === INVITATIONS_COLLECTION) {
        return createCollection<InvitationDocument>({ data: state.invitations }) as unknown as Collection<TSchema>;
      }
      if (name === TASKS_COLLECTION) {
        return createCollection<TaskDocument>({ data: state.tasks }) as unknown as Collection<TSchema>;
      }
      if (name === NODES_COLLECTION) {
        return createCollection<NodeDocument>({ data: state.nodes }) as unknown as Collection<TSchema>;
      }
      if (name === AUDIT_COLLECTION) {
        return createCollection<AuditLog>({ data: state.audits }) as unknown as Collection<TSchema>;
      }

      throw new Error(`Unexpected collection: ${name}`);
    },
  };
  return db as unknown as Db;
};

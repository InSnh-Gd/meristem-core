import { randomUUID } from 'crypto';
import bcrypt from 'bcrypt';
import type { Db } from 'mongodb';

import { USERS_COLLECTION, type UserDocument } from '../db/collections';
import { ensureRolesBelongToOrg, resolvePermissionsByRoleIds } from './role';

const BCRYPT_SALT_ROUNDS = 12;

const uniqueStrings = (values: string[]): string[] => Array.from(new Set(values));

export type UserPublicView = {
  user_id: string;
  username: string;
  role_ids: string[];
  org_id: string;
  permissions: string[];
  permissions_v: number;
  created_at: string;
  updated_at: string;
};

export type ListUsersOptions = {
  orgId?: string;
  limit: number;
  offset: number;
};

export type CreateUserInput = {
  username: string;
  password: string;
  org_id: string;
  role_ids: string[];
};

export type UpdateUserInput = {
  username?: string;
  org_id?: string;
};

export const toUserPublicView = (user: UserDocument): UserPublicView => ({
  user_id: user.user_id,
  username: user.username,
  role_ids: user.role_ids,
  org_id: user.org_id,
  permissions: user.permissions,
  permissions_v: user.permissions_v,
  created_at: user.created_at.toISOString(),
  updated_at: user.updated_at.toISOString(),
});

export const getUserById = async (db: Db, userId: string): Promise<UserDocument | null> => {
  const collection = db.collection<UserDocument>(USERS_COLLECTION);
  return collection.findOne({ user_id: userId });
};

export const getUserByUsername = async (db: Db, username: string): Promise<UserDocument | null> => {
  const collection = db.collection<UserDocument>(USERS_COLLECTION);
  return collection.findOne({ username });
};

export const listUsers = async (
  db: Db,
  options: ListUsersOptions,
): Promise<{ data: UserDocument[]; total: number }> => {
  const collection = db.collection<UserDocument>(USERS_COLLECTION);
  const filter = options.orgId ? { org_id: options.orgId } : {};
  const total = await collection.countDocuments(filter);
  const data = await collection
    .find(filter)
    .sort({ created_at: 1 })
    .skip(options.offset)
    .limit(options.limit)
    .toArray();
  return { data, total };
};

const computeUserPermissions = async (
  db: Db,
  orgId: string,
  roleIds: string[],
): Promise<string[]> => {
  const permissions = await resolvePermissionsByRoleIds(db, orgId, roleIds);
  return uniqueStrings(permissions);
};

export const createUser = async (db: Db, input: CreateUserInput): Promise<UserDocument> => {
  const collection = db.collection<UserDocument>(USERS_COLLECTION);
  const existing = await collection.findOne({ username: input.username });
  if (existing) {
    throw new Error('USER_ALREADY_EXISTS');
  }

  const roleIds = uniqueStrings(input.role_ids);
  const rolesOk = await ensureRolesBelongToOrg(db, input.org_id, roleIds);
  if (!rolesOk) {
    throw new Error('ROLE_ORG_MISMATCH');
  }

  const now = new Date();
  const password_hash = await bcrypt.hash(input.password, BCRYPT_SALT_ROUNDS);
  const permissions = await computeUserPermissions(db, input.org_id, roleIds);
  const user: UserDocument = {
    user_id: randomUUID(),
    username: input.username,
    password_hash,
    role_ids: roleIds,
    org_id: input.org_id,
    permissions,
    permissions_v: 1,
    tokens: [],
    created_at: now,
    updated_at: now,
  };

  await collection.insertOne(user);
  return user;
};

export const updateUser = async (
  db: Db,
  userId: string,
  input: UpdateUserInput,
): Promise<UserDocument | null> => {
  const collection = db.collection<UserDocument>(USERS_COLLECTION);
  const current = await collection.findOne({ user_id: userId });
  if (!current) {
    return null;
  }

  if (typeof input.username === 'string' && input.username !== current.username) {
    const duplicated = await collection.findOne({ username: input.username });
    if (duplicated) {
      throw new Error('USER_ALREADY_EXISTS');
    }
  }

  const targetOrgId = typeof input.org_id === 'string' ? input.org_id : current.org_id;
  const rolesOk = await ensureRolesBelongToOrg(db, targetOrgId, current.role_ids);
  if (!rolesOk) {
    throw new Error('ROLE_ORG_MISMATCH');
  }

  const permissions = await computeUserPermissions(db, targetOrgId, current.role_ids);
  const update: {
    $set: Record<string, unknown>;
  } = {
    $set: {
      updated_at: new Date(),
      org_id: targetOrgId,
      permissions,
      permissions_v: current.permissions_v + 1,
    },
  };

  if (typeof input.username === 'string') {
    update.$set.username = input.username;
  }

  const updated = await collection.findOneAndUpdate(
    { user_id: userId },
    update,
    { returnDocument: 'after' },
  );
  return updated;
};

export const deleteUser = async (db: Db, userId: string): Promise<boolean> => {
  const collection = db.collection<UserDocument>(USERS_COLLECTION);
  const result = await collection.deleteOne({ user_id: userId });
  return result.deletedCount > 0;
};

export const syncUserPermissions = async (
  db: Db,
  userId: string,
): Promise<UserDocument | null> => {
  const collection = db.collection<UserDocument>(USERS_COLLECTION);
  const user = await collection.findOne({ user_id: userId });
  if (!user) {
    return null;
  }

  const permissions = await computeUserPermissions(db, user.org_id, user.role_ids);
  const updated = await collection.findOneAndUpdate(
    { user_id: userId },
    {
      $set: {
        permissions,
        permissions_v: user.permissions_v + 1,
        updated_at: new Date(),
      },
    },
    { returnDocument: 'after' },
  );
  return updated;
};

export const assignRoleToUser = async (
  db: Db,
  userId: string,
  roleId: string,
): Promise<UserDocument | null> => {
  const collection = db.collection<UserDocument>(USERS_COLLECTION);
  const user = await collection.findOne({ user_id: userId });
  if (!user) {
    return null;
  }

  const nextRoleIds = uniqueStrings([...user.role_ids, roleId]);
  const rolesOk = await ensureRolesBelongToOrg(db, user.org_id, nextRoleIds);
  if (!rolesOk) {
    throw new Error('ROLE_ORG_MISMATCH');
  }

  const permissions = await computeUserPermissions(db, user.org_id, nextRoleIds);
  const updated = await collection.findOneAndUpdate(
    { user_id: userId },
    {
      $set: {
        role_ids: nextRoleIds,
        permissions,
        permissions_v: user.permissions_v + 1,
        updated_at: new Date(),
      },
    },
    { returnDocument: 'after' },
  );
  return updated;
};

export const removeRoleFromUser = async (
  db: Db,
  userId: string,
  roleId: string,
): Promise<UserDocument | null> => {
  const collection = db.collection<UserDocument>(USERS_COLLECTION);
  const user = await collection.findOne({ user_id: userId });
  if (!user) {
    return null;
  }

  const nextRoleIds = user.role_ids.filter((candidate) => candidate !== roleId);
  const permissions = await computeUserPermissions(db, user.org_id, nextRoleIds);
  const updated = await collection.findOneAndUpdate(
    { user_id: userId },
    {
      $set: {
        role_ids: nextRoleIds,
        permissions,
        permissions_v: user.permissions_v + 1,
        updated_at: new Date(),
      },
    },
    { returnDocument: 'after' },
  );
  return updated;
};

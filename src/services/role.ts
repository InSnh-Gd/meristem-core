import { randomUUID } from 'crypto';
import type { Db } from 'mongodb';

import { ROLES_COLLECTION, type RoleDocument } from '../db/collections';

const uniqueStrings = (values: string[]): string[] => Array.from(new Set(values));

const normalizePermissions = (permissions: string[]): string[] =>
  uniqueStrings(
    permissions
      .map((item) => item.trim())
      .filter((item) => item.length > 0),
  );

export type ListRolesOptions = {
  orgId?: string;
  limit: number;
  offset: number;
};

export type CreateRoleInput = {
  name: string;
  description: string;
  permissions: string[];
  org_id: string;
};

export type UpdateRoleInput = {
  name?: string;
  description?: string;
  permissions?: string[];
};

export const listRoles = async (
  db: Db,
  options: ListRolesOptions,
): Promise<{ data: RoleDocument[]; total: number }> => {
  const collection = db.collection<RoleDocument>(ROLES_COLLECTION);
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

export const findRoleById = async (db: Db, roleId: string): Promise<RoleDocument | null> => {
  const collection = db.collection<RoleDocument>(ROLES_COLLECTION);
  return collection.findOne({ role_id: roleId });
};

export const createRole = async (db: Db, input: CreateRoleInput): Promise<RoleDocument> => {
  const collection = db.collection<RoleDocument>(ROLES_COLLECTION);
  const duplicated = await collection.findOne({
    org_id: input.org_id,
    name: input.name,
  });
  if (duplicated) {
    throw new Error('ROLE_NAME_CONFLICT');
  }

  const now = new Date();
  const role: RoleDocument = {
    role_id: randomUUID(),
    name: input.name,
    description: input.description,
    permissions: normalizePermissions(input.permissions),
    is_builtin: false,
    org_id: input.org_id,
    created_at: now,
    updated_at: now,
  };
  await collection.insertOne(role);
  return role;
};

export const updateRole = async (
  db: Db,
  roleId: string,
  input: UpdateRoleInput,
): Promise<RoleDocument | null> => {
  const collection = db.collection<RoleDocument>(ROLES_COLLECTION);
  const current = await collection.findOne({ role_id: roleId });
  if (!current) {
    return null;
  }
  if (current.is_builtin) {
    throw new Error('ROLE_BUILTIN_READONLY');
  }

  if (typeof input.name === 'string' && input.name !== current.name) {
    const duplicated = await collection.findOne({
      org_id: current.org_id,
      name: input.name,
      role_id: { $ne: roleId },
    });
    if (duplicated) {
      throw new Error('ROLE_NAME_CONFLICT');
    }
  }

  const update: {
    $set: Record<string, unknown>;
  } = {
    $set: {
      updated_at: new Date(),
    },
  };
  if (typeof input.name === 'string') {
    update.$set.name = input.name;
  }
  if (typeof input.description === 'string') {
    update.$set.description = input.description;
  }
  if (Array.isArray(input.permissions)) {
    update.$set.permissions = normalizePermissions(input.permissions);
  }

  const updated = await collection.findOneAndUpdate(
    { role_id: roleId },
    update,
    { returnDocument: 'after' },
  );
  return updated;
};

export const deleteRole = async (
  db: Db,
  roleId: string,
): Promise<boolean> => {
  const collection = db.collection<RoleDocument>(ROLES_COLLECTION);
  const current = await collection.findOne({ role_id: roleId });
  if (!current) {
    return false;
  }
  if (current.is_builtin) {
    throw new Error('ROLE_BUILTIN_READONLY');
  }

  const result = await collection.deleteOne({ role_id: roleId });
  return result.deletedCount > 0;
};

export const resolvePermissionsByRoleIds = async (
  db: Db,
  orgId: string,
  roleIds: string[],
): Promise<string[]> => {
  if (roleIds.length === 0) {
    return [];
  }

  const collection = db.collection<RoleDocument>(ROLES_COLLECTION);
  const roles = await collection.find({
    org_id: orgId,
    role_id: { $in: roleIds },
  }).toArray();

  const permissions = roles.flatMap((role) => role.permissions);
  return normalizePermissions(permissions);
};

export const ensureRolesBelongToOrg = async (
  db: Db,
  orgId: string,
  roleIds: string[],
): Promise<boolean> => {
  if (roleIds.length === 0) {
    return true;
  }
  const collection = db.collection<RoleDocument>(ROLES_COLLECTION);
  const roles = await collection.find({
    org_id: orgId,
    role_id: { $in: roleIds },
  }).toArray();
  return roles.length === uniqueStrings(roleIds).length;
};

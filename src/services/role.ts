import { randomUUID } from 'crypto';
import type { Db } from 'mongodb';
import type { DbSession } from '../db/transactions';
import {
  decodeCreatedAtCursor,
  encodeCreatedAtCursor,
  normalizeCursorPagination,
} from '../db/query-policy';
import {
  deleteRoleById,
  findRoleById as findRoleByIdRepo,
  findRoleByOrgAndName,
  findRoleByOrgAndNameExcludingId,
  findRolesByOrgAndIds,
  insertRole,
  listRoles as listRolesRepo,
  updateRoleById,
} from '../db/repositories/roles';
import type { RoleDocument } from '../db/collections';
import { DomainError } from '../errors/domain-error';

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
  cursor?: string;
};

type CursorPageInfo = {
  has_next: boolean;
  next_cursor: string | null;
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
): Promise<{ data: RoleDocument[]; page_info: CursorPageInfo }> => {
  const pagination = normalizeCursorPagination(
    {
      limit: options.limit,
      cursor: options.cursor,
    },
    {
      defaultLimit: 100,
      maxLimit: 200,
    },
  );
  const cursor = pagination.cursor
    ? decodeCreatedAtCursor(pagination.cursor)
    : null;
  const filter = options.orgId ? { org_id: options.orgId } : {};
  const rows = await listRolesRepo(db, {
    filter,
    limit: pagination.limit,
    cursor,
    session: null,
  });
  const hasNext = rows.length > pagination.limit;
  const data = hasNext ? rows.slice(0, pagination.limit) : rows;
  const last = data.at(-1);

  return {
    data,
    page_info: {
      has_next: hasNext,
      next_cursor:
        hasNext && last
          ? encodeCreatedAtCursor({
              createdAt: last.created_at,
              tieBreaker: last.role_id,
            })
          : null,
    },
  };
};

export const findRoleById = async (
  db: Db,
  roleId: string,
): Promise<RoleDocument | null> => findRoleByIdRepo(db, roleId);

export const createRole = async (
  db: Db,
  input: CreateRoleInput,
): Promise<RoleDocument> => {
  const duplicated = await findRoleByOrgAndName(db, input.org_id, input.name);
  if (duplicated) {
    throw new DomainError('ROLE_NAME_CONFLICT');
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

  await insertRole(db, role);
  return role;
};

export const updateRole = async (
  db: Db,
  roleId: string,
  input: UpdateRoleInput,
): Promise<RoleDocument | null> => {
  const current = await findRoleByIdRepo(db, roleId);
  if (!current) {
    return null;
  }
  if (current.is_builtin) {
    throw new DomainError('ROLE_BUILTIN_READONLY');
  }

  if (typeof input.name === 'string' && input.name !== current.name) {
    const duplicated = await findRoleByOrgAndNameExcludingId(
      db,
      current.org_id,
      input.name,
      roleId,
    );
    if (duplicated) {
      throw new DomainError('ROLE_NAME_CONFLICT');
    }
  }

  const updateSet: Partial<RoleDocument> = {
    updated_at: new Date(),
  };

  if (typeof input.name === 'string') {
    updateSet.name = input.name;
  }
  if (typeof input.description === 'string') {
    updateSet.description = input.description;
  }
  if (Array.isArray(input.permissions)) {
    updateSet.permissions = normalizePermissions(input.permissions);
  }

  return updateRoleById(db, roleId, { $set: updateSet });
};

export const deleteRole = async (
  db: Db,
  roleId: string,
): Promise<boolean> => {
  const current = await findRoleByIdRepo(db, roleId);
  if (!current) {
    return false;
  }
  if (current.is_builtin) {
    throw new DomainError('ROLE_BUILTIN_READONLY');
  }

  const deletedCount = await deleteRoleById(db, roleId);
  return deletedCount > 0;
};

export const resolvePermissionsByRoleIds = async (
  db: Db,
  orgId: string,
  roleIds: string[],
  session: DbSession = null,
): Promise<string[]> => {
  if (roleIds.length === 0) {
    return [];
  }

  const roles = await findRolesByOrgAndIds(
    db,
    orgId,
    roleIds,
    session,
  );
  const permissions = roles.flatMap((role) => role.permissions);
  return normalizePermissions(permissions);
};

export const ensureRolesBelongToOrg = async (
  db: Db,
  orgId: string,
  roleIds: string[],
  session: DbSession = null,
): Promise<boolean> => {
  if (roleIds.length === 0) {
    return true;
  }
  const normalizedRoleIds = uniqueStrings(roleIds);
  const roles = await findRolesByOrgAndIds(
    db,
    orgId,
    normalizedRoleIds,
    session,
  );
  return roles.length === normalizedRoleIds.length;
};

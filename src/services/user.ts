import { randomUUID } from 'crypto';
import bcrypt from 'bcrypt';
import type { Db } from 'mongodb';
import type { DbSession } from '../db/transactions';
import { runInTransaction } from '../db/transactions';
import { normalizePagination } from '../db/query-policy';
import {
  countUsers,
  deleteUserById,
  findUserById as findUserByIdRepo,
  findUserByUsername,
  findUserByUsernameExcludingId,
  insertUser,
  listUsers as listUsersRepo,
  updateUserById,
} from '../db/repositories/users';
import type { UserDocument } from '../db/collections';
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

export const getUserById = async (
  db: Db,
  userId: string,
  session: DbSession = null,
): Promise<UserDocument | null> => findUserByIdRepo(db, userId, session);

export const getUserByUsername = async (
  db: Db,
  username: string,
  session: DbSession = null,
): Promise<UserDocument | null> => findUserByUsername(db, username, session);

export const listUsers = async (
  db: Db,
  options: ListUsersOptions,
): Promise<{ data: UserDocument[]; total: number }> => {
  const pagination = normalizePagination(
    {
      limit: options.limit,
      offset: options.offset,
    },
    {
      defaultLimit: 100,
      maxLimit: 200,
    },
  );
  const filter = options.orgId ? { org_id: options.orgId } : {};
  const [total, data] = await Promise.all([
    countUsers(db, filter),
    listUsersRepo(db, {
      filter,
      limit: pagination.limit,
      offset: pagination.offset,
      session: null,
    }),
  ]);

  return { data, total };
};

const computeUserPermissions = async (
  db: Db,
  orgId: string,
  roleIds: string[],
  session: DbSession = null,
): Promise<string[]> => {
  const permissions = await resolvePermissionsByRoleIds(
    db,
    orgId,
    roleIds,
    session,
  );
  return uniqueStrings(permissions);
};

export const createUser = async (
  db: Db,
  input: CreateUserInput,
  session: DbSession = null,
): Promise<UserDocument> => {
  const existing = await findUserByUsername(db, input.username, session);
  if (existing) {
    throw new Error('USER_ALREADY_EXISTS');
  }

  const roleIds = uniqueStrings(input.role_ids);
  const rolesOk = await ensureRolesBelongToOrg(
    db,
    input.org_id,
    roleIds,
    session,
  );
  if (!rolesOk) {
    throw new Error('ROLE_ORG_MISMATCH');
  }

  const now = new Date();
  const password_hash = await bcrypt.hash(input.password, BCRYPT_SALT_ROUNDS);
  const permissions = await computeUserPermissions(
    db,
    input.org_id,
    roleIds,
    session,
  );
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

  await insertUser(db, user, session);
  return user;
};

export const updateUser = async (
  db: Db,
  userId: string,
  input: UpdateUserInput,
): Promise<UserDocument | null> => {
  const current = await findUserByIdRepo(db, userId);
  if (!current) {
    return null;
  }

  if (typeof input.username === 'string' && input.username !== current.username) {
    const duplicated = await findUserByUsernameExcludingId(
      db,
      input.username,
      userId,
    );
    if (duplicated) {
      throw new Error('USER_ALREADY_EXISTS');
    }
  }

  const targetOrgId =
    typeof input.org_id === 'string' ? input.org_id : current.org_id;
  const rolesOk = await ensureRolesBelongToOrg(
    db,
    targetOrgId,
    current.role_ids,
  );
  if (!rolesOk) {
    throw new Error('ROLE_ORG_MISMATCH');
  }

  const permissions = await computeUserPermissions(
    db,
    targetOrgId,
    current.role_ids,
  );

  const updateSet: Partial<UserDocument> = {
    updated_at: new Date(),
    org_id: targetOrgId,
    permissions,
    permissions_v: current.permissions_v + 1,
  };

  if (typeof input.username === 'string') {
    updateSet.username = input.username;
  }

  return updateUserById(db, userId, {
    $set: updateSet,
  });
};

export const deleteUser = async (db: Db, userId: string): Promise<boolean> => {
  const deletedCount = await deleteUserById(db, userId);
  return deletedCount > 0;
};

export const syncUserPermissions = async (
  db: Db,
  userId: string,
): Promise<UserDocument | null> => {
  const user = await findUserByIdRepo(db, userId);
  if (!user) {
    return null;
  }

  const permissions = await computeUserPermissions(
    db,
    user.org_id,
    user.role_ids,
  );

  return updateUserById(db, userId, {
    $set: {
      permissions,
      permissions_v: user.permissions_v + 1,
      updated_at: new Date(),
    },
  });
};

export const assignRoleToUser = async (
  db: Db,
  userId: string,
  roleId: string,
): Promise<UserDocument | null> =>
  runInTransaction(db, async (session) => {
    const user = await findUserByIdRepo(db, userId, session);
    if (!user) {
      return null;
    }

    const nextRoleIds = uniqueStrings([...user.role_ids, roleId]);
    const rolesOk = await ensureRolesBelongToOrg(
      db,
      user.org_id,
      nextRoleIds,
      session,
    );
    if (!rolesOk) {
      throw new Error('ROLE_ORG_MISMATCH');
    }

    const permissions = await computeUserPermissions(
      db,
      user.org_id,
      nextRoleIds,
      session,
    );

    return updateUserById(
      db,
      userId,
      {
        $set: {
          role_ids: nextRoleIds,
          permissions,
          permissions_v: user.permissions_v + 1,
          updated_at: new Date(),
        },
      },
      session,
    );
  });

export const removeRoleFromUser = async (
  db: Db,
  userId: string,
  roleId: string,
): Promise<UserDocument | null> =>
  runInTransaction(db, async (session) => {
    const user = await findUserByIdRepo(db, userId, session);
    if (!user) {
      return null;
    }

    const nextRoleIds = user.role_ids.filter((candidate) => candidate !== roleId);
    const permissions = await computeUserPermissions(
      db,
      user.org_id,
      nextRoleIds,
      session,
    );

    return updateUserById(
      db,
      userId,
      {
        $set: {
          role_ids: nextRoleIds,
          permissions,
          permissions_v: user.permissions_v + 1,
          updated_at: new Date(),
        },
      },
      session,
    );
  });

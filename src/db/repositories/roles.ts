import type {
  Db,
  Filter,
  UpdateFilter,
  FindOneAndUpdateOptions,
} from 'mongodb';
import { ROLES_COLLECTION, type RoleDocument } from '../collections';
import type { DbSession } from '../transactions';
import { resolveQueryMaxTimeMs } from '../query-policy';
import { toSessionOption } from './shared';

type RoleListInput = {
  filter: Filter<RoleDocument>;
  limit: number;
  offset: number;
  session: DbSession;
};

const rolesCollection = (db: Db) => db.collection<RoleDocument>(ROLES_COLLECTION);
const QUERY_MAX_TIME_MS = resolveQueryMaxTimeMs();

export const findRoleById = async (
  db: Db,
  roleId: string,
  session: DbSession = null,
): Promise<RoleDocument | null> =>
  rolesCollection(db).findOne(
    { role_id: roleId },
    toSessionOption(session),
  );

export const findRoleByOrgAndName = async (
  db: Db,
  orgId: string,
  name: string,
  session: DbSession = null,
): Promise<RoleDocument | null> =>
  rolesCollection(db).findOne(
    { org_id: orgId, name },
    toSessionOption(session),
  );

export const findRoleByOrgAndNameExcludingId = async (
  db: Db,
  orgId: string,
  name: string,
  roleId: string,
  session: DbSession = null,
): Promise<RoleDocument | null> =>
  rolesCollection(db).findOne(
    {
      org_id: orgId,
      name,
      role_id: { $ne: roleId },
    },
    toSessionOption(session),
  );

export const findRolesByOrgAndIds = async (
  db: Db,
  orgId: string,
  roleIds: string[],
  session: DbSession = null,
): Promise<RoleDocument[]> =>
  rolesCollection(db)
    .find(
      {
        org_id: orgId,
        role_id: { $in: roleIds },
      },
      toSessionOption(session),
    )
    .toArray();

export const countRoles = async (
  db: Db,
  filter: Filter<RoleDocument>,
  session: DbSession = null,
): Promise<number> =>
  rolesCollection(db).countDocuments(filter, {
    ...toSessionOption(session),
    maxTimeMS: QUERY_MAX_TIME_MS,
  });

export const listRoles = async (
  db: Db,
  input: RoleListInput,
): Promise<RoleDocument[]> =>
  rolesCollection(db)
    .find(input.filter, toSessionOption(input.session))
    .sort({ created_at: 1 })
    .skip(input.offset)
    .limit(input.limit)
    .maxTimeMS(QUERY_MAX_TIME_MS)
    .toArray();

export const insertRole = async (
  db: Db,
  role: RoleDocument,
  session: DbSession = null,
): Promise<void> => {
  await rolesCollection(db).insertOne(role, toSessionOption(session));
};

export const updateRoleById = async (
  db: Db,
  roleId: string,
  update: UpdateFilter<RoleDocument>,
  session: DbSession = null,
): Promise<RoleDocument | null> => {
  const options: FindOneAndUpdateOptions = {
    returnDocument: 'after',
    ...toSessionOption(session),
  };
  return rolesCollection(db).findOneAndUpdate(
    { role_id: roleId },
    update,
    options,
  );
};

export const deleteRoleById = async (
  db: Db,
  roleId: string,
  session: DbSession = null,
): Promise<number> => {
  const result = await rolesCollection(db).deleteOne(
    { role_id: roleId },
    toSessionOption(session),
  );
  return result.deletedCount;
};

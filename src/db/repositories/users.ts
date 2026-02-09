import type {
  Db,
  Filter,
  UpdateFilter,
  FindOneAndUpdateOptions,
} from 'mongodb';
import { USERS_COLLECTION, type UserDocument } from '../collections';
import type { DbSession } from '../transactions';
import type { CreatedAtCursor } from '../query-policy';
import { resolveQueryMaxTimeMs } from '../query-policy';
import {
  applyCreatedAtCursorFilter,
  executeRepositoryOperation,
  toSessionOption,
} from './shared';

type UserListInput = {
  filter: Filter<UserDocument>;
  limit: number;
  cursor: CreatedAtCursor | null;
  session: DbSession;
};

const usersCollection = (db: Db) => db.collection<UserDocument>(USERS_COLLECTION);
const QUERY_MAX_TIME_MS = resolveQueryMaxTimeMs();

export const findUserById = async (
  db: Db,
  userId: string,
  session: DbSession = null,
): Promise<UserDocument | null> =>
  executeRepositoryOperation(
    USERS_COLLECTION,
    'find_user_by_id',
    () => usersCollection(db).findOne(
      { user_id: userId },
      toSessionOption(session),
    ),
  );

export const findUserByUsername = async (
  db: Db,
  username: string,
  session: DbSession = null,
): Promise<UserDocument | null> =>
  executeRepositoryOperation(
    USERS_COLLECTION,
    'find_user_by_username',
    () => usersCollection(db).findOne(
      { username },
      toSessionOption(session),
    ),
  );

export const findUserByUsernameExcludingId = async (
  db: Db,
  username: string,
  userId: string,
  session: DbSession = null,
): Promise<UserDocument | null> =>
  executeRepositoryOperation(
    USERS_COLLECTION,
    'find_user_by_username_excluding_id',
    () => usersCollection(db).findOne(
      {
        username,
        user_id: { $ne: userId },
      },
      toSessionOption(session),
    ),
  );

export const countUsers = async (
  db: Db,
  filter: Filter<UserDocument>,
  session: DbSession = null,
): Promise<number> =>
  executeRepositoryOperation(
    USERS_COLLECTION,
    'count_users',
    () => usersCollection(db).countDocuments(filter, {
      ...toSessionOption(session),
      maxTimeMS: QUERY_MAX_TIME_MS,
    }),
  );

export const listUsers = async (
  db: Db,
  input: UserListInput,
): Promise<UserDocument[]> =>
  executeRepositoryOperation(
    USERS_COLLECTION,
    'list_users',
    () => usersCollection(db)
      .find(
        applyCreatedAtCursorFilter(
          input.filter,
          input.cursor,
          'user_id',
        ),
        toSessionOption(input.session),
      )
      .sort({ created_at: 1, user_id: 1 })
      .limit(input.limit + 1)
      .maxTimeMS(QUERY_MAX_TIME_MS)
      .toArray(),
  );

export const insertUser = async (
  db: Db,
  user: UserDocument,
  session: DbSession = null,
): Promise<void> => {
  await executeRepositoryOperation(
    USERS_COLLECTION,
    'insert_user',
    () => usersCollection(db).insertOne(user, toSessionOption(session)),
  );
};

export const updateUserById = async (
  db: Db,
  userId: string,
  update: UpdateFilter<UserDocument>,
  session: DbSession = null,
): Promise<UserDocument | null> => {
  const options: FindOneAndUpdateOptions = {
    returnDocument: 'after',
    ...toSessionOption(session),
  };
  return executeRepositoryOperation(
    USERS_COLLECTION,
    'update_user_by_id',
    () => usersCollection(db).findOneAndUpdate(
      { user_id: userId },
      update,
      options,
    ),
  );
};

export const deleteUserById = async (
  db: Db,
  userId: string,
  session: DbSession = null,
): Promise<number> => {
  const result = await executeRepositoryOperation(
    USERS_COLLECTION,
    'delete_user_by_id',
    () => usersCollection(db).deleteOne(
      { user_id: userId },
      toSessionOption(session),
    ),
  );
  return result.deletedCount;
};

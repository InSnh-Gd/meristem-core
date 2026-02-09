import type {
  Db,
  Filter,
  UpdateFilter,
  FindOneAndUpdateOptions,
} from 'mongodb';
import { USERS_COLLECTION, type UserDocument } from '../collections';
import type { DbSession } from '../transactions';
import { resolveQueryMaxTimeMs } from '../query-policy';
import { toSessionOption } from './shared';

type UserListInput = {
  filter: Filter<UserDocument>;
  limit: number;
  offset: number;
  session: DbSession;
};

const usersCollection = (db: Db) => db.collection<UserDocument>(USERS_COLLECTION);
const QUERY_MAX_TIME_MS = resolveQueryMaxTimeMs();

export const findUserById = async (
  db: Db,
  userId: string,
  session: DbSession = null,
): Promise<UserDocument | null> =>
  usersCollection(db).findOne(
    { user_id: userId },
    toSessionOption(session),
  );

export const findUserByUsername = async (
  db: Db,
  username: string,
  session: DbSession = null,
): Promise<UserDocument | null> =>
  usersCollection(db).findOne(
    { username },
    toSessionOption(session),
  );

export const findUserByUsernameExcludingId = async (
  db: Db,
  username: string,
  userId: string,
  session: DbSession = null,
): Promise<UserDocument | null> =>
  usersCollection(db).findOne(
    {
      username,
      user_id: { $ne: userId },
    },
    toSessionOption(session),
  );

export const countUsers = async (
  db: Db,
  filter: Filter<UserDocument>,
  session: DbSession = null,
): Promise<number> =>
  usersCollection(db).countDocuments(filter, {
    ...toSessionOption(session),
    maxTimeMS: QUERY_MAX_TIME_MS,
  });

export const listUsers = async (
  db: Db,
  input: UserListInput,
): Promise<UserDocument[]> =>
  usersCollection(db)
    .find(input.filter, toSessionOption(input.session))
    .sort({ created_at: 1 })
    .skip(input.offset)
    .limit(input.limit)
    .maxTimeMS(QUERY_MAX_TIME_MS)
    .toArray();

export const insertUser = async (
  db: Db,
  user: UserDocument,
  session: DbSession = null,
): Promise<void> => {
  await usersCollection(db).insertOne(user, toSessionOption(session));
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
  return usersCollection(db).findOneAndUpdate(
    { user_id: userId },
    update,
    options,
  );
};

export const deleteUserById = async (
  db: Db,
  userId: string,
  session: DbSession = null,
): Promise<number> => {
  const result = await usersCollection(db).deleteOne(
    { user_id: userId },
    toSessionOption(session),
  );
  return result.deletedCount;
};

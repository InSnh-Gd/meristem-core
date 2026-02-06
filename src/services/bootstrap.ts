import { randomBytes, randomUUID } from 'crypto';
import bcrypt from 'bcrypt';
import type { Db } from 'mongodb';
import { USERS_COLLECTION, type UserDocument } from '../db/collections';

const BOOTSTRAP_TOKEN_PREFIX = 'ST';
const BOOTSTRAP_TOKEN_REGEX = /^ST-[A-Z0-9]{4}-[A-Z0-9]{4}$/;
const BOOTSTRAP_TOKEN_CHARSET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
const BOOTSTRAP_TOKEN_CHUNK_SIZE = 4;
const BCRYPT_SALT_ROUNDS = 12;

const randomBootstrapChunk = (): string => {
  const bytes = randomBytes(BOOTSTRAP_TOKEN_CHUNK_SIZE);
  return Array.from(bytes)
    .map((value) => BOOTSTRAP_TOKEN_CHARSET[value % BOOTSTRAP_TOKEN_CHARSET.length])
    .join('');
};

/**
 * Generate a bootstrap token in the format ST-XXXX-XXXX using uppercase alphanumerics.
 */
export const generateBootstrapToken = (): string =>
  `${BOOTSTRAP_TOKEN_PREFIX}-${randomBootstrapChunk()}-${randomBootstrapChunk()}`;

/**
 * Validate that the provided token matches the expected bootstrap token format.
 */
export const validateBootstrapToken = (token: string): boolean => BOOTSTRAP_TOKEN_REGEX.test(token);

/**
 * Create the very first admin user (bootstrap) with a bcrypt-hashed password.
 * This should only succeed when the users collection is empty.
 */
export const createFirstUser = async (db: Db, username: string, password: string): Promise<UserDocument> => {
  const collection = db.collection<UserDocument>(USERS_COLLECTION);
  const existingUsers = await collection.countDocuments();
  if (existingUsers > 0) {
    throw new Error('bootstrap already completed');
  }

  const password_hash = await bcrypt.hash(password, BCRYPT_SALT_ROUNDS);
  const now = new Date();
  const user: UserDocument = {
    user_id: randomUUID(),
    username,
    password_hash,
    is_admin: true,
    permissions: [],
    permissions_v: 1,
    tokens: [],
    created_at: now,
    updated_at: now,
  };

  await collection.insertOne(user);
  return user;
};

import { randomBytes, randomUUID } from 'crypto';
import bcrypt from 'bcrypt';
import type { Db } from 'mongodb';
import {
  ORGS_COLLECTION,
  ROLES_COLLECTION,
  USERS_COLLECTION,
  type OrgDocument,
  type RoleDocument,
  type UserDocument,
} from '../db/collections';

export const DEFAULT_ORG_ID = 'org-default';
export const SUPERADMIN_ROLE_ID = 'role-superadmin';
export const SUPERADMIN_ROLE_NAME = 'superadmin';

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

const ensureDefaultOrgAndRole = async (db: Db): Promise<void> => {
  const orgCollection = db.collection<OrgDocument>(ORGS_COLLECTION);
  const roleCollection = db.collection<RoleDocument>(ROLES_COLLECTION);
  const now = new Date();

  const org = await orgCollection.findOne({ org_id: DEFAULT_ORG_ID });
  if (!org) {
    await orgCollection.insertOne({
      org_id: DEFAULT_ORG_ID,
      name: 'Default Organization',
      slug: 'default',
      owner_user_id: '',
      settings: {},
      created_at: now,
      updated_at: now,
    });
  }

  const superadminRole = await roleCollection.findOne({ role_id: SUPERADMIN_ROLE_ID });
  if (!superadminRole) {
    await roleCollection.insertOne({
      role_id: SUPERADMIN_ROLE_ID,
      name: SUPERADMIN_ROLE_NAME,
      description: 'Built-in superadmin role',
      permissions: ['*'],
      is_builtin: true,
      org_id: DEFAULT_ORG_ID,
      created_at: now,
      updated_at: now,
    });
  }
};

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

  await ensureDefaultOrgAndRole(db);

  const password_hash = await bcrypt.hash(password, BCRYPT_SALT_ROUNDS);
  const now = new Date();
  const user: UserDocument = {
    user_id: randomUUID(),
    username,
    password_hash,
    role_ids: [SUPERADMIN_ROLE_ID],
    org_id: DEFAULT_ORG_ID,
    permissions: ['*'],
    permissions_v: 1,
    tokens: [],
    created_at: now,
    updated_at: now,
  };

  await collection.insertOne(user);
  await db.collection<OrgDocument>(ORGS_COLLECTION).updateOne(
    { org_id: DEFAULT_ORG_ID },
    {
      $set: {
        owner_user_id: user.user_id,
        updated_at: now,
      },
    },
  );
  return user;
};

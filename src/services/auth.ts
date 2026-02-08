import bcrypt from 'bcrypt';
import type { Db } from 'mongodb';
import { jwt as createJwtPlugin } from '@elysiajs/jwt';
import { getJwtSignSecret } from '../config';
import { USERS_COLLECTION, type UserDocument } from '../db/collections';
import { SUPERADMIN_ROLE_ID } from './bootstrap';

const JWT_EXPIRATION_SECONDS = 24 * 60 * 60;

let jwtSignerState: { signer: ReturnType<typeof createJwtPlugin>; secret: string } | null = null;

const getJwtSigner = (): ReturnType<typeof createJwtPlugin> => {
  const secret = getJwtSignSecret();
  if (!secret) {
    throw new Error('JWT secret is not configured');
  }

  if (jwtSignerState && jwtSignerState.secret === secret) {
    return jwtSignerState.signer;
  }

  const signer = createJwtPlugin({ secret });
  jwtSignerState = { signer, secret };
  return signer;
};

/**
 * Verify a username/password pair against the users collection.
 *
 * Returns the full user document only when the password matches.
 */
export const authenticateUser = async (db: Db, username: string, password: string): Promise<UserDocument | null> => {
  const collection = db.collection<UserDocument>(USERS_COLLECTION);
  const user = await collection.findOne({ username });

  if (!user) {
    return null;
  }

  const passwordMatches = await bcrypt.compare(password, user.password_hash);
  return passwordMatches ? user : null;
};

/**
 * Generate an HS256 JWT that expires after 24 hours.
 *
 * The token carries the user ID, role flag, permissions list, and standard claims.
 */
export const generateJWT = async (user: UserDocument): Promise<string> => {
  const signer = getJwtSigner();
  const now = Math.floor(Date.now() / 1000);
  const expiresAt = now + JWT_EXPIRATION_SECONDS;
  const roleIds = Array.isArray(user.role_ids) ? user.role_ids : [];
  const permissions = Array.isArray(user.permissions) ? user.permissions : [];
  const isSuperadmin = roleIds.includes(SUPERADMIN_ROLE_ID) || permissions.includes('*');

  const payload = {
    sub: user.user_id,
    user_id: user.user_id,
    role: isSuperadmin ? 'admin' : 'user',
    type: 'USER' as const,
    permissions,
    exp: expiresAt,
  };

  return signer.decorator.jwt.sign(payload);
};

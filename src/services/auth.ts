import bcrypt from 'bcrypt';
import type { Db } from 'mongodb';
import { jwt as createJwtPlugin } from '@elysiajs/jwt';
import { getJwtSecret } from '../config';
import { USERS_COLLECTION, type UserDocument } from '../db/collections';

const JWT_EXPIRATION_SECONDS = 24 * 60 * 60;

let jwtSigner: ReturnType<typeof createJwtPlugin> | null = null;

const getJwtSigner = (): ReturnType<typeof createJwtPlugin> => {
  if (jwtSigner) {
    return jwtSigner;
  }

  const secret = getJwtSecret();
  if (!secret) {
    throw new Error('JWT secret is not configured');
  }

  jwtSigner = createJwtPlugin({ secret });
  return jwtSigner;
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

  const payload = {
    sub: user.user_id,
    user_id: user.user_id,
    role: user.is_admin ? 'admin' : 'user',
    type: 'USER' as const,
    permissions: user.permissions,
    exp: expiresAt,
  };

  return signer.decorator.jwt.sign(payload);
};

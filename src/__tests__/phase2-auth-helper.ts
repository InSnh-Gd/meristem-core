import { SignJWT } from 'jose';

const encodeSecret = (value: string): Uint8Array => new TextEncoder().encode(value);

export const createBearerToken = async (
  payload: {
    sub: string;
    type: 'USER' | 'NODE' | 'PLUGIN';
    permissions: string[];
    exp?: number;
    node_id?: string;
  },
  secret: string,
): Promise<string> => {
  const now = Math.floor(Date.now() / 1000);
  const exp = payload.exp ?? now + 60 * 60;
  const token = await new SignJWT({
    sub: payload.sub,
    type: payload.type,
    permissions: payload.permissions,
    node_id: payload.node_id,
    exp,
    iat: now,
  })
    .setProtectedHeader({ alg: 'HS256' })
    .sign(encodeSecret(secret));
  return `Bearer ${token}`;
};

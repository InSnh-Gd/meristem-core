import { createCipheriv, createDecipheriv, pbkdf2Sync, randomBytes } from 'crypto';

const AES_ALGORITHM = 'aes-256-gcm';
const SALT_LENGTH_BYTES = 16;
const IV_LENGTH_BYTES = 16;
const AUTH_TAG_LENGTH_BYTES = 16;
const DATA_KEY_LENGTH_BYTES = 32;
const PBKDF2_ITERATIONS = 100_000;
const PBKDF2_DIGEST = 'sha256';

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const packEncryptedPayload = (
  salt: Buffer,
  iv: Buffer,
  authTag: Buffer,
  ciphertext: Buffer,
): Buffer => Buffer.concat([salt, iv, authTag, ciphertext]);

const deriveEncryptionKey = (secret: string, salt: Buffer): Buffer =>
  pbkdf2Sync(secret, salt, PBKDF2_ITERATIONS, DATA_KEY_LENGTH_BYTES, PBKDF2_DIGEST);

const encryptConfig = (data: Record<string, unknown>, secret: string): Buffer => {
  const salt = randomBytes(SALT_LENGTH_BYTES);
  const iv = randomBytes(IV_LENGTH_BYTES);
  const key = deriveEncryptionKey(secret, salt);
  const cipher = createCipheriv(AES_ALGORITHM, key, iv);
  const plaintext = Buffer.from(JSON.stringify(data), 'utf8');
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const authTag = cipher.getAuthTag();

  return packEncryptedPayload(salt, iv, authTag, ciphertext);
};

const decryptConfig = (encrypted: Buffer, secret: string): Record<string, unknown> => {
  const metadataLength = SALT_LENGTH_BYTES + IV_LENGTH_BYTES + AUTH_TAG_LENGTH_BYTES;
  if (encrypted.length < metadataLength) {
    throw new Error('Encrypted payload is too short');
  }

  const saltEnd = SALT_LENGTH_BYTES;
  const ivEnd = saltEnd + IV_LENGTH_BYTES;
  const authTagEnd = ivEnd + AUTH_TAG_LENGTH_BYTES;

  const salt = encrypted.subarray(0, saltEnd);
  const iv = encrypted.subarray(saltEnd, ivEnd);
  const authTag = encrypted.subarray(ivEnd, authTagEnd);
  const ciphertext = encrypted.subarray(authTagEnd);

  const key = deriveEncryptionKey(secret, salt);
  const decipher = createDecipheriv(AES_ALGORITHM, key, iv);
  decipher.setAuthTag(authTag);

  const plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
  const parsed: unknown = JSON.parse(plaintext.toString('utf8'));

  if (!isRecord(parsed)) {
    throw new Error('Decrypted config is not an object');
  }

  return parsed;
};

const generateDataKey = (): string => randomBytes(DATA_KEY_LENGTH_BYTES).toString('hex');

export { encryptConfig, decryptConfig, generateDataKey };

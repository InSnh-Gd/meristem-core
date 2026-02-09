import type { ClientSession } from 'mongodb';
import type { DbSession } from '../transactions';

export const toSessionOption = (
  session: DbSession,
): { session?: ClientSession } => (session ? { session } : {});

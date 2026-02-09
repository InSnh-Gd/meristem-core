import type { Db, Filter } from 'mongodb';
import {
  INVITATIONS_COLLECTION,
  type InvitationDocument,
} from '../collections';
import type { DbSession } from '../transactions';
import { executeRepositoryOperation, toSessionOption } from './shared';

const invitationsCollection = (db: Db) =>
  db.collection<InvitationDocument>(INVITATIONS_COLLECTION);

export const insertInvitation = async (
  db: Db,
  invitation: InvitationDocument,
  session: DbSession = null,
): Promise<void> => {
  await executeRepositoryOperation(
    INVITATIONS_COLLECTION,
    'insert_invitation',
    () => invitationsCollection(db).insertOne(
      invitation,
      toSessionOption(session),
    ),
  );
};

export const findInvitation = async (
  db: Db,
  filter: Filter<InvitationDocument>,
  session: DbSession = null,
): Promise<InvitationDocument | null> =>
  executeRepositoryOperation(
    INVITATIONS_COLLECTION,
    'find_invitation',
    () => invitationsCollection(db).findOne(filter, toSessionOption(session)),
  );

export const markInvitationExpired = async (
  db: Db,
  invitationId: string,
  now: Date,
  session: DbSession = null,
): Promise<number> => {
  const result = await executeRepositoryOperation(
    INVITATIONS_COLLECTION,
    'mark_invitation_expired',
    () => invitationsCollection(db).updateOne(
      {
        invitation_id: invitationId,
        status: 'pending',
      },
      {
        $set: {
          status: 'expired',
          updated_at: now,
        },
      },
      toSessionOption(session),
    ),
  );
  return result.modifiedCount;
};

export const markInvitationAccepted = async (
  db: Db,
  invitationId: string,
  now: Date,
  session: DbSession = null,
): Promise<number> => {
  const result = await executeRepositoryOperation(
    INVITATIONS_COLLECTION,
    'mark_invitation_accepted',
    () => invitationsCollection(db).updateOne(
      {
        invitation_id: invitationId,
        status: 'pending',
      },
      {
        $set: {
          status: 'accepted',
          accepted_at: now,
          updated_at: now,
        },
      },
      toSessionOption(session),
    ),
  );
  return result.modifiedCount;
};

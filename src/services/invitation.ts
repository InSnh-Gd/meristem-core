import { randomUUID } from 'crypto';
import type { Db } from 'mongodb';

import { INVITATIONS_COLLECTION, type InvitationDocument } from '../db/collections';
import { createUser } from './user';

const DEFAULT_EXPIRES_IN_HOURS = 24;

const uniqueStrings = (values: string[]): string[] => Array.from(new Set(values));

export type CreateInvitationInput = {
  username: string;
  org_id: string;
  role_ids: string[];
  created_by: string;
  expires_in_hours?: number;
};

export type AcceptInvitationInput = {
  invitation_token: string;
  password: string;
};

export const createInvitation = async (
  db: Db,
  input: CreateInvitationInput,
): Promise<InvitationDocument> => {
  const collection = db.collection<InvitationDocument>(INVITATIONS_COLLECTION);
  const now = new Date();
  const expiresInHours =
    typeof input.expires_in_hours === 'number' && input.expires_in_hours > 0
      ? input.expires_in_hours
      : DEFAULT_EXPIRES_IN_HOURS;

  const invitation: InvitationDocument = {
    invitation_id: randomUUID(),
    invitation_token: randomUUID(),
    username: input.username,
    org_id: input.org_id,
    role_ids: uniqueStrings(input.role_ids),
    created_by: input.created_by,
    status: 'pending',
    expires_at: new Date(now.getTime() + expiresInHours * 60 * 60 * 1000),
    created_at: now,
    updated_at: now,
  };

  await collection.insertOne(invitation);
  return invitation;
};

export const acceptInvitation = async (
  db: Db,
  input: AcceptInvitationInput,
): Promise<{ user_id: string }> => {
  const collection = db.collection<InvitationDocument>(INVITATIONS_COLLECTION);
  const invitation = await collection.findOne({
    invitation_token: input.invitation_token,
  });

  if (!invitation) {
    throw new Error('INVITATION_NOT_FOUND');
  }
  if (invitation.status === 'accepted') {
    throw new Error('INVITATION_ALREADY_ACCEPTED');
  }

  const now = new Date();
  if (invitation.expires_at.getTime() <= now.getTime()) {
    await collection.updateOne(
      { invitation_id: invitation.invitation_id },
      {
        $set: {
          status: 'expired',
          updated_at: now,
        },
      },
    );
    throw new Error('INVITATION_EXPIRED');
  }

  const user = await createUser(db, {
    username: invitation.username,
    password: input.password,
    org_id: invitation.org_id,
    role_ids: invitation.role_ids,
  });

  await collection.updateOne(
    { invitation_id: invitation.invitation_id },
    {
      $set: {
        status: 'accepted',
        accepted_at: now,
        updated_at: now,
      },
    },
  );

  return { user_id: user.user_id };
};

import { randomUUID } from 'crypto';
import type { Db } from 'mongodb';

import type { InvitationDocument } from '../db/collections';
import {
  findInvitation,
  insertInvitation,
  markInvitationAccepted,
  markInvitationExpired,
} from '../db/repositories/invitations';
import { runInTransaction } from '../db/transactions';
import { DomainError } from '../errors/domain-error';
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

  await insertInvitation(db, invitation);
  return invitation;
};

export const acceptInvitation = async (
  db: Db,
  input: AcceptInvitationInput,
): Promise<{ user_id: string }> =>
  runInTransaction(db, async (session) => {
    const invitation = await findInvitation(
      db,
      {
        invitation_token: input.invitation_token,
      },
      session,
    );

    if (!invitation) {
      throw new DomainError('INVITATION_NOT_FOUND');
    }
    if (invitation.status === 'accepted') {
      throw new DomainError('INVITATION_ALREADY_ACCEPTED');
    }

    const now = new Date();
    if (invitation.expires_at.getTime() <= now.getTime()) {
      await markInvitationExpired(
        db,
        invitation.invitation_id,
        now,
        session,
      );
      throw new DomainError('INVITATION_EXPIRED');
    }

    const user = await createUser(
      db,
      {
        username: invitation.username,
        password: input.password,
        org_id: invitation.org_id,
        role_ids: invitation.role_ids,
      },
      session,
    );

    const modifiedCount = await markInvitationAccepted(
      db,
      invitation.invitation_id,
      now,
      session,
    );
    if (modifiedCount === 0) {
      throw new DomainError('INVITATION_ALREADY_ACCEPTED');
    }

    return { user_id: user.user_id };
  });

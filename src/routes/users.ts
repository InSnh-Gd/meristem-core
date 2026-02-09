import { Elysia, t } from 'elysia';
import type { Db } from 'mongodb';

import { requireAuth, type AuthStore } from '../middleware/auth';
import { DEFAULT_ORG_ID } from '../services/bootstrap';
import { createInvitation, acceptInvitation } from '../services/invitation';
import { ensureRolesBelongToOrg } from '../services/role';
import { ensureSuperadminAccess } from './route-auth';
import { respondWithCode, respondWithError } from './route-errors';
import {
  assignRoleToUser,
  createUser,
  deleteUser,
  getUserById,
  listUsers,
  removeRoleFromUser,
  toUserPublicView,
  updateUser,
} from '../services/user';

const UserPublicSchema = t.Object({
  user_id: t.String(),
  username: t.String(),
  role_ids: t.Array(t.String()),
  org_id: t.String(),
  permissions: t.Array(t.String()),
  permissions_v: t.Number(),
  created_at: t.String(),
  updated_at: t.String(),
});

const GenericErrorSchema = t.Object({
  success: t.Literal(false),
  error: t.String(),
});

const GenericSuccessSchema = t.Object({
  success: t.Literal(true),
});

const UsersListResponseSchema = t.Object({
  success: t.Literal(true),
  data: t.Array(UserPublicSchema),
  page_info: t.Object({
    has_next: t.Boolean(),
    next_cursor: t.Union([t.String(), t.Null()]),
  }),
});

const UserSingleResponseSchema = t.Object({
  success: t.Literal(true),
  data: UserPublicSchema,
});

const UserCreateRequestSchema = t.Object({
  username: t.String({ minLength: 1 }),
  password: t.String({ minLength: 8 }),
  org_id: t.Optional(t.String({ minLength: 1 })),
  role_ids: t.Optional(t.Array(t.String())),
});

const UserCreateResponseSchema = t.Object({
  success: t.Literal(true),
  user_id: t.String(),
});

const UserUpdateRequestSchema = t.Object({
  username: t.Optional(t.String({ minLength: 1 })),
  org_id: t.Optional(t.String({ minLength: 1 })),
});

const UsersQuerySchema = t.Object({
  org_id: t.Optional(t.String({ minLength: 1 })),
  limit: t.Optional(t.Numeric({ minimum: 1, maximum: 200 })),
  cursor: t.Optional(t.String({ minLength: 1 })),
});

const UserInvitationCreateRequestSchema = t.Object({
  username: t.String({ minLength: 1 }),
  org_id: t.Optional(t.String({ minLength: 1 })),
  role_ids: t.Optional(t.Array(t.String())),
  expires_in_hours: t.Optional(t.Numeric({ minimum: 1, maximum: 24 * 30 })),
});

const UserInvitationCreateResponseSchema = t.Object({
  success: t.Literal(true),
  invitation_id: t.String(),
  invitation_token: t.String(),
  expires_at: t.String(),
});

const UserInvitationAcceptRequestSchema = t.Object({
  invitation_token: t.String({ minLength: 1 }),
  password: t.String({ minLength: 8 }),
});

const UserInvitationAcceptResponseSchema = t.Object({
  success: t.Literal(true),
  user_id: t.String(),
});

const RoleAssignRequestSchema = t.Object({
  role_id: t.String({ minLength: 1 }),
});

const RoleAssignResponseSchema = t.Object({
  success: t.Literal(true),
  data: UserPublicSchema,
});

export const usersRoute = (app: Elysia, db: Db): Elysia => {
  app.get(
    '/api/v1/users',
    async ({ query, set, store }) => {
      const denied = ensureSuperadminAccess(store, set);
      if (denied) {
        return denied;
      }

      try {
        const { data, page_info } = await listUsers(db, {
          orgId: query.org_id,
          limit: query.limit ?? 100,
          cursor: query.cursor,
        });

        return {
          success: true,
          data: data.map((user) => toUserPublicView(user)),
          page_info,
        };
      } catch (error) {
        return respondWithError(set, error);
      }
    },
    {
      query: UsersQuerySchema,
      response: {
        200: UsersListResponseSchema,
        400: GenericErrorSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  app.get(
    '/api/v1/users/:id',
    async ({ params, set, store }) => {
      const denied = ensureSuperadminAccess(store, set);
      if (denied) {
        return denied;
      }

      const user = await getUserById(db, params.id);
      if (!user) {
        set.status = 404;
        return {
          success: false,
          error: 'NOT_FOUND',
        };
      }

      return {
        success: true,
        data: toUserPublicView(user),
      };
    },
    {
      response: {
        200: UserSingleResponseSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        404: GenericErrorSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  app.post(
    '/api/v1/users',
    async ({ body, set, store }) => {
      const denied = ensureSuperadminAccess(store, set);
      if (denied) {
        return denied;
      }

      const orgId = body.org_id ?? DEFAULT_ORG_ID;
      const roleIds = body.role_ids ?? [];
      try {
        const user = await createUser(db, {
          username: body.username,
          password: body.password,
          org_id: orgId,
          role_ids: roleIds,
        });
        set.status = 201;
        return {
          success: true,
          user_id: user.user_id,
        };
      } catch (error) {
        return respondWithError(set, error);
      }
    },
    {
      body: UserCreateRequestSchema,
      response: {
        201: UserCreateResponseSchema,
        400: GenericErrorSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        409: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  app.patch(
    '/api/v1/users/:id',
    async ({ params, body, set, store }) => {
      const denied = ensureSuperadminAccess(store, set);
      if (denied) {
        return denied;
      }

      try {
        const user = await updateUser(db, params.id, {
          username: body.username,
          org_id: body.org_id,
        });
        if (!user) {
          set.status = 404;
          return {
            success: false,
            error: 'NOT_FOUND',
          };
        }
        return {
          success: true,
          data: toUserPublicView(user),
        };
      } catch (error) {
        return respondWithError(set, error);
      }
    },
    {
      body: UserUpdateRequestSchema,
      response: {
        200: UserSingleResponseSchema,
        400: GenericErrorSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        404: GenericErrorSchema,
        409: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  app.delete(
    '/api/v1/users/:id',
    async ({ params, set, store }) => {
      const denied = ensureSuperadminAccess(store, set);
      if (denied) {
        return denied;
      }

      const removed = await deleteUser(db, params.id);
      if (!removed) {
        set.status = 404;
        return {
          success: false,
          error: 'NOT_FOUND',
        };
      }
      return { success: true };
    },
    {
      response: {
        200: GenericSuccessSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        404: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  app.post(
    '/api/v1/users/invitations',
    async ({ body, set, store }) => {
      const denied = ensureSuperadminAccess(store, set);
      if (denied) {
        return denied;
      }

      const authStore = store as AuthStore;
      const orgId = body.org_id ?? DEFAULT_ORG_ID;
      const roleIds = body.role_ids ?? [];
      const rolesOk = await ensureRolesBelongToOrg(db, orgId, roleIds);
      if (!rolesOk) {
        return respondWithCode(set, 'ROLE_ORG_MISMATCH');
      }

      const invitation = await createInvitation(db, {
        username: body.username,
        org_id: orgId,
        role_ids: roleIds,
        created_by: authStore.user.id,
        expires_in_hours: body.expires_in_hours,
      });

      set.status = 201;
      return {
        success: true,
        invitation_id: invitation.invitation_id,
        invitation_token: invitation.invitation_token,
        expires_at: invitation.expires_at.toISOString(),
      };
    },
    {
      body: UserInvitationCreateRequestSchema,
      response: {
        201: UserInvitationCreateResponseSchema,
        400: GenericErrorSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  app.post(
    '/api/v1/users/invitations/accept',
    async ({ body, set }) => {
      try {
        const result = await acceptInvitation(db, {
          invitation_token: body.invitation_token,
          password: body.password,
        });
        set.status = 201;
        return {
          success: true,
          user_id: result.user_id,
        };
      } catch (error) {
        return respondWithError(set, error);
      }
    },
    {
      body: UserInvitationAcceptRequestSchema,
      response: {
        201: UserInvitationAcceptResponseSchema,
        400: GenericErrorSchema,
        404: GenericErrorSchema,
        409: GenericErrorSchema,
        410: GenericErrorSchema,
        500: GenericErrorSchema,
      },
    },
  );

  app.post(
    '/api/v1/users/:id/roles',
    async ({ params, body, set, store }) => {
      const denied = ensureSuperadminAccess(store, set);
      if (denied) {
        return denied;
      }

      try {
        const user = await assignRoleToUser(db, params.id, body.role_id);
        if (!user) {
          set.status = 404;
          return {
            success: false,
            error: 'NOT_FOUND',
          };
        }
        return {
          success: true,
          data: toUserPublicView(user),
        };
      } catch (error) {
        return respondWithError(set, error);
      }
    },
    {
      body: RoleAssignRequestSchema,
      response: {
        200: RoleAssignResponseSchema,
        400: GenericErrorSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        404: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  app.delete(
    '/api/v1/users/:id/roles/:roleId',
    async ({ params, set, store }) => {
      const denied = ensureSuperadminAccess(store, set);
      if (denied) {
        return denied;
      }

      const user = await removeRoleFromUser(db, params.id, params.roleId);
      if (!user) {
        set.status = 404;
        return {
          success: false,
          error: 'NOT_FOUND',
        };
      }
      return {
        success: true,
        data: toUserPublicView(user),
      };
    },
    {
      response: {
        200: RoleAssignResponseSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        404: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  return app;
};

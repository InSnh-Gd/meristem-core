import { Elysia, t } from 'elysia';
import type { Db } from 'mongodb';

import { requireAuth } from '../middleware/auth';
import { DEFAULT_ORG_ID } from '../services/bootstrap';
import {
  createRole,
  deleteRole,
  findRoleById,
  listRoles,
  type UpdateRoleInput,
  updateRole,
} from '../services/role';
import { requireSuperadmin } from './route-auth';
import { respondWithCode, respondWithError } from './route-errors';

const GenericErrorSchema = t.Object({
  success: t.Literal(false),
  error: t.String(),
});

const RoleSchema = t.Object({
  role_id: t.String(),
  name: t.String(),
  description: t.String(),
  permissions: t.Array(t.String()),
  is_builtin: t.Boolean(),
  org_id: t.String(),
  created_at: t.String(),
  updated_at: t.String(),
});

const RoleListResponseSchema = t.Object({
  success: t.Literal(true),
  data: t.Array(RoleSchema),
  page_info: t.Object({
    has_next: t.Boolean(),
    next_cursor: t.Union([t.String(), t.Null()]),
  }),
});

const RoleSingleResponseSchema = t.Object({
  success: t.Literal(true),
  data: RoleSchema,
});

const RoleCreateRequestSchema = t.Object({
  name: t.String({ minLength: 1 }),
  description: t.String(),
  permissions: t.Array(t.String()),
  org_id: t.Optional(t.String({ minLength: 1 })),
});

const RoleCreateResponseSchema = t.Object({
  success: t.Literal(true),
  role_id: t.String(),
});

const RoleUpdateRequestSchema = t.Object({
  name: t.Optional(t.String({ minLength: 1 })),
  description: t.Optional(t.String()),
  permissions: t.Optional(t.Array(t.String())),
});

const RoleListQuerySchema = t.Object({
  org_id: t.Optional(t.String({ minLength: 1 })),
  limit: t.Optional(t.Numeric({ minimum: 1, maximum: 200 })),
  cursor: t.Optional(t.String({ minLength: 1 })),
});

const GenericSuccessSchema = t.Object({
  success: t.Literal(true),
});

const toRoleView = (role: {
  role_id: string;
  name: string;
  description: string;
  permissions: string[];
  is_builtin: boolean;
  org_id: string;
  created_at: Date;
  updated_at: Date;
}) => ({
  role_id: role.role_id,
  name: role.name,
  description: role.description,
  permissions: role.permissions,
  is_builtin: role.is_builtin,
  org_id: role.org_id,
  created_at: role.created_at.toISOString(),
  updated_at: role.updated_at.toISOString(),
});

export const rolesRoute = (app: Elysia, db: Db): Elysia => {
  app.get(
    '/api/v1/roles',
    async ({ query, set }) => {
      try {
        const { data, page_info } = await listRoles(db, {
          orgId: query.org_id,
          limit: query.limit ?? 100,
          cursor: query.cursor,
        });
        return {
          success: true,
          data: data.map((role) => toRoleView(role)),
          page_info,
        };
      } catch (error) {
        return respondWithError(set, error);
      }
    },
    {
      query: RoleListQuerySchema,
      response: {
        200: RoleListResponseSchema,
        400: GenericErrorSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth, requireSuperadmin],
    },
  );

  app.get(
    '/api/v1/roles/:id',
    async ({ params, set }) => {
      const role = await findRoleById(db, params.id);
      if (!role) {
        return respondWithCode(set, 'NOT_FOUND');
      }
      return {
        success: true,
        data: toRoleView(role),
      };
    },
    {
      response: {
        200: RoleSingleResponseSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        404: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth, requireSuperadmin],
    },
  );

  app.post(
    '/api/v1/roles',
    async ({ body, set }) => {
      try {
        const role = await createRole(db, {
          name: body.name,
          description: body.description,
          permissions: body.permissions,
          org_id: body.org_id ?? DEFAULT_ORG_ID,
        });
        set.status = 201;
        return {
          success: true,
          role_id: role.role_id,
        };
      } catch (error) {
        return respondWithError(set, error);
      }
    },
    {
      body: RoleCreateRequestSchema,
      response: {
        201: RoleCreateResponseSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        409: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth, requireSuperadmin],
    },
  );

  app.patch(
    '/api/v1/roles/:id',
    async ({ params, body, set }) => {
      try {
        const updateInput: UpdateRoleInput = {
          name: body.name,
          description: body.description,
          permissions: body.permissions,
        };
        const role = await updateRole(db, params.id, updateInput);
        if (!role) {
          return respondWithCode(set, 'NOT_FOUND');
        }
        return {
          success: true,
          data: toRoleView(role),
        };
      } catch (error) {
        return respondWithError(set, error);
      }
    },
    {
      body: RoleUpdateRequestSchema,
      response: {
        200: RoleSingleResponseSchema,
        400: GenericErrorSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        404: GenericErrorSchema,
        409: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth, requireSuperadmin],
    },
  );

  app.delete(
    '/api/v1/roles/:id',
    async ({ params, set }) => {
      try {
        const removed = await deleteRole(db, params.id);
        if (!removed) {
          return respondWithCode(set, 'NOT_FOUND');
        }
        return {
          success: true,
        };
      } catch (error) {
        return respondWithError(set, error);
      }
    },
    {
      response: {
        200: GenericSuccessSchema,
        400: GenericErrorSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        404: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth, requireSuperadmin],
    },
  );

  return app;
};

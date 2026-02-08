import { Elysia, t } from 'elysia';
import type { Db } from 'mongodb';

import { requireAuth, type AuthStore } from '../middleware/auth';
import { DEFAULT_ORG_ID } from '../services/bootstrap';
import {
  createRole,
  deleteRole,
  findRoleById,
  listRoles,
  type UpdateRoleInput,
  updateRole,
} from '../services/role';

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
  total: t.Number(),
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
  offset: t.Optional(t.Numeric({ minimum: 0 })),
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

const ensureSuperadmin = (
  context: { set: { status?: unknown }; store: Record<string, unknown> },
): { success: false; error: string } | null => {
  const store = context.store as unknown as AuthStore;
  if (!store.user) {
    context.set.status = 401;
    return {
      success: false,
      error: 'UNAUTHORIZED',
    };
  }
  if (!store.user.permissions.includes('*')) {
    context.set.status = 403;
    return {
      success: false,
      error: 'ACCESS_DENIED',
    };
  }
  return null;
};

export const rolesRoute = (app: Elysia, db: Db): Elysia => {
  app.get(
    '/api/v1/roles',
    async ({ query, set, store }) => {
      const denied = ensureSuperadmin({ set, store });
      if (denied) {
        return denied;
      }

      const { data, total } = await listRoles(db, {
        orgId: query.org_id,
        limit: query.limit ?? 100,
        offset: query.offset ?? 0,
      });
      return {
        success: true,
        data: data.map((role) => toRoleView(role)),
        total,
      };
    },
    {
      query: RoleListQuerySchema,
      response: {
        200: RoleListResponseSchema,
        401: GenericErrorSchema,
        403: GenericErrorSchema,
        500: GenericErrorSchema,
      },
      beforeHandle: [requireAuth],
    },
  );

  app.get(
    '/api/v1/roles/:id',
    async ({ params, set, store }) => {
      const denied = ensureSuperadmin({ set, store });
      if (denied) {
        return denied;
      }

      const role = await findRoleById(db, params.id);
      if (!role) {
        set.status = 404;
        return {
          success: false,
          error: 'NOT_FOUND',
        };
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
      beforeHandle: [requireAuth],
    },
  );

  app.post(
    '/api/v1/roles',
    async ({ body, set, store }) => {
      const denied = ensureSuperadmin({ set, store });
      if (denied) {
        return denied;
      }

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
        if (error instanceof Error && error.message === 'ROLE_NAME_CONFLICT') {
          set.status = 409;
          return {
            success: false,
            error: 'ROLE_NAME_CONFLICT',
          };
        }
        set.status = 500;
        return {
          success: false,
          error: 'INTERNAL_ERROR',
        };
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
      beforeHandle: [requireAuth],
    },
  );

  app.patch(
    '/api/v1/roles/:id',
    async ({ params, body, set, store }) => {
      const denied = ensureSuperadmin({ set, store });
      if (denied) {
        return denied;
      }

      try {
        const updateInput: UpdateRoleInput = {
          name: body.name,
          description: body.description,
          permissions: body.permissions,
        };
        const role = await updateRole(db, params.id, updateInput);
        if (!role) {
          set.status = 404;
          return {
            success: false,
            error: 'NOT_FOUND',
          };
        }
        return {
          success: true,
          data: toRoleView(role),
        };
      } catch (error) {
        if (error instanceof Error && error.message === 'ROLE_BUILTIN_READONLY') {
          set.status = 400;
          return {
            success: false,
            error: 'ROLE_BUILTIN_READONLY',
          };
        }
        if (error instanceof Error && error.message === 'ROLE_NAME_CONFLICT') {
          set.status = 409;
          return {
            success: false,
            error: 'ROLE_NAME_CONFLICT',
          };
        }
        set.status = 500;
        return {
          success: false,
          error: 'INTERNAL_ERROR',
        };
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
      beforeHandle: [requireAuth],
    },
  );

  app.delete(
    '/api/v1/roles/:id',
    async ({ params, set, store }) => {
      const denied = ensureSuperadmin({ set, store });
      if (denied) {
        return denied;
      }

      try {
        const removed = await deleteRole(db, params.id);
        if (!removed) {
          set.status = 404;
          return {
            success: false,
            error: 'NOT_FOUND',
          };
        }
        return {
          success: true,
        };
      } catch (error) {
        if (error instanceof Error && error.message === 'ROLE_BUILTIN_READONLY') {
          set.status = 400;
          return {
            success: false,
            error: 'ROLE_BUILTIN_READONLY',
          };
        }
        set.status = 500;
        return {
          success: false,
          error: 'INTERNAL_ERROR',
        };
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
      beforeHandle: [requireAuth],
    },
  );

  return app;
};

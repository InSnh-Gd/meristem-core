# Meristem Core Agent Guide

## Scope
- This guide applies only to `meristem-core/`.
- Keep behavior aligned with root `AGENTS.md`.

## Runtime and Commands
- Runtime: Bun.
- Dev server: `bun run dev`
- Start server: `bun run start`
- Build: `bun run build`

## Engineering Constraints
- Bun-only workflow: use `bun install`, `bun run`, `bun test`, `bun publish`.
- Do not use `npm`/`yarn`/`pnpm`.
- Do not use Node.js runtime flows unless strictly necessary and explicitly approved.
- No `any`; use `unknown` + type guards.
- Keep TypeScript strict semantics; do not weaken strict checks.
- Avoid OOP/Java-style implementation; prefer FP/composition.
- For Elysia routes/middleware/plugins, keep composable FP style.
- Run LSP/type diagnostics after TypeScript edits.
- For uncertain external facts, use search MCP and cite evidence.

## Architecture Boundaries
- `meristem-core` stays pure: connection, scheduling, plugin loading.
- Do not add business-domain logic into Core routes/services.
- Keep explicit context passing (`TraceContext`), no hidden async context state.

## Canonical Terminology
- Use `AGENT` and `GIG` as canonical persona names.
- Do not introduce deprecated role terminology in code or docs.

## Current API Baseline
- Health: `GET /health` (configurable by `healthRoute`)
- Join: `POST /api/v1/join`
- Audit: `GET /api/v1/audit-logs` (auth + `sys:audit`)
- Auth bootstrap: `POST /api/v1/auth/bootstrap`
- Auth login: `POST /api/v1/auth/login`
- Tasks: `POST /api/v1/tasks` (auth)
- Results: `POST /api/v1/results`
- Note: no WebSocket route is currently registered in `src/index.ts`.

## NATS and Logging Baseline
- Core subscribes to heartbeat: `meristem.v1.hb.>` in `src/index.ts`.
- JetStream logs stream initialized by `src/services/jetstream-setup.ts`.
- Log subjects:
  - `meristem.v1.logs.sys.[nodeId]`
  - `meristem.v1.logs.task.[nodeId].[taskId]`
- Key logging files:
  - `src/utils/logger.ts`
  - `src/utils/nats-transport.ts`

## Key Files
- Entry: `src/index.ts`
- Routes: `src/routes/*.ts`
- Auth/RBAC middleware: `src/middleware/auth.ts`, `src/middleware/rbac.ts`
- NATS connection: `src/nats/connection.ts`
- Trace context: `src/utils/trace-context.ts`

## Validation
- Preferred full test run from project root: `bun test`.
- Package-local checks: `bun test meristem-core/src/__tests__`.
- Keep docs in sync when API/Event subjects change (`docs/standards/*`, `docs/specs/*`).

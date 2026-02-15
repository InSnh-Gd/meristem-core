# Meristem Core (Standalone Repository)

This repository can run as a standalone Core checkout. It does not require the full development workspace layout.

## 1) Runtime Requirements

- Bun `>= 1.0.0`
- MongoDB instance reachable from Core
- NATS server (JetStream enabled) reachable from Core

## 2) Dependency Setup

Install dependencies from this repository directly:

```bash
bun install
```

## 3) Startup Modes

```bash
# Development mode (legacy behavior, no MERISTEM_HOME isolation)
bun run start:dev

# Production mode (CLI-managed, single-binary compatible)
bun run start
```

## 4) Core + Plugin Notes

- Core hosts plugin lifecycle APIs under `/api/v1/plugins`.
- Client nodes connect to Core and NATS remotely; clients should not host a local NATS server in the default topology.
- Plugin discovery/install/update is handled by the built-in CLI:

```bash
bun run cli -- -Sy
bun run cli -- -Ss
bun run cli -- -S com.meristem.mnet
bun run cli -- -Q
bun run cli -- -Qk
```

- Plugin install root defaults to `MERISTEM_HOME/plugins` where `MERISTEM_HOME` is resolved as:
  1. `--home <path>`
  2. `MERISTEM_HOME`
  3. compile-time embedded core path
- Development mode keeps legacy default plugin base path `/plugins` unless `MERISTEM_PLUGIN_BASE_PATH` is explicitly set.

## 5) Single Binary

```bash
bun run build:bin
./dist/meristem-core -h
./dist/meristem-core -Ss
./dist/meristem-core -S com.meristem.mnet
```

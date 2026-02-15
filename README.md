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

## 3) Minimal Startup

```bash
bun run start
```

## 4) Core + Plugin Notes

- Core hosts plugin lifecycle APIs under `/api/v1/plugins`.
- Client nodes connect to Core and NATS remotely; clients should not host a local NATS server in the default topology.
- Plugin discovery/install/update is handled by the built-in CLI:

```bash
bun run cli -- plugin refresh
bun run cli -- plugin list --available
bun run cli -- plugin sync --plugin com.meristem.mnet
```

- Plugin install root defaults to `MERISTEM_HOME/plugins` where `MERISTEM_HOME` is resolved as:
  1. `--home <path>`
  2. `MERISTEM_HOME`
  3. compile-time embedded core path

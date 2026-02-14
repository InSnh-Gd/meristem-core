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
bun run build
MERISTEM_DATABASE_MONGO_URI=mongodb://127.0.0.1:27017/meristem \
MERISTEM_NATS_URL=nats://127.0.0.1:4222 \
MERISTEM_SERVER_PORT=3000 \
bun run dist/index.js
```

## 4) Core + Plugin Notes

- Core hosts plugin lifecycle APIs under `/api/v1/plugins`.
- Client nodes connect to Core and NATS remotely; clients should not host a local NATS server in the default topology.

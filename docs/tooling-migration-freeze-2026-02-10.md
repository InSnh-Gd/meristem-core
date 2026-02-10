# Tooling Migration Freeze (2026-02-10)

## 目标
- 冻结本轮“测试/基准/E2E 外置”迁移差异，确保 PR 审阅时能快速区分保留项、修正项和去耦项。
- 本文只记录 `meristem-core` 侧与外置编排相关的变更，不覆盖业务功能改动。

## 关联分支
- Repository: `meristem-core`
- Branch: `feat/core-db-phase2-hardening`

## 关联提交（与本轮迁移直接相关）
- `38f0014` `chore(core): route benchmark and integration commands to tooling repo`
- `a4ac52a` `chore(core): switch tooling commands to jsr package binary`
- `bf1a13f` `chore: wire meristem-tooling jsr dependency and cli entry`
- `2951f11` `chore(core): decouple tooling commands from node_modules path`

## 差异分类

### 保留项（Keep）
- `@insnh-gd/meristem-tooling` 作为 `devDependencies`，通过 JSR/NPM 映射引入。
- `benchmark:run:*` 与 `test:run:integration:core` 继续由 `meristem-core` 暴露，便于现有使用习惯平滑迁移。

### 修正项（Fix）
- 将 `package.json` 中对 `node_modules/@insnh-gd/meristem-tooling/src/cli.js` 的直连调用替换为 CLI 桥接入口 `src/cli/tooling.ts`。
- 将 workspace 根目录定位约定收敛到 `MERISTEM_WORKSPACE_ROOT`，并在桥接入口提供默认值。

### 去耦项（Remove Coupling）
- 移除对 `node_modules` 物理目录结构的运行时依赖。
- 禁止在提交态保留 `file:../meristem-shared` / `file:../meristem-tooling` 本地依赖链路。

## 当前已知阻塞
- `@insnh-gd/meristem-shared` 已发布版本尚未包含 `WIRE_CONTRACT_VERSION` / `parseWsPushMessage` 新导出时，workspace 全量测试会出现导出缺失错误。
- 该问题由 shared PR 分支补丁解决，需在 shared 侧合并并发布后消除。

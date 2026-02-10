#!/usr/bin/env bun

/**
 * 这个桥接入口用于把 core 仓的测试/基准命令稳定委托给 meristem-tooling。
 * 这样做的目的有两点：
 * 1) 避免在 package.json 里硬编码 node_modules 物理路径，降低仓库拆分后的耦合风险；
 * 2) 固化 MERISTEM_WORKSPACE_ROOT 的默认值，保证从 core 仓直接执行时仍能定位工作区根目录。
 *
 * 降级路径：
 * - 当 tooling 包缺失、导出异常或加载失败时，立即输出可读错误并非 0 退出，防止命令“假成功”。
 */
const runToolingBridge = async (): Promise<void> => {
  const workspaceRoot = process.env.MERISTEM_WORKSPACE_ROOT?.trim();
  if (!workspaceRoot) {
    process.env.MERISTEM_WORKSPACE_ROOT = '..';
  }

  /**
   * 这里使用子进程转发而不是直接 import CLI：
   * - JSR 包产物中的 `cli.d.ts` 是脚本声明，直接动态导入会触发 TS 模块检查报错；
   * - 通过 Bun 子进程执行同一入口，可以保持命令行为一致，同时规避类型系统对“脚本模块”的误判。
   *
   * 失败路径：
   * - 子进程启动失败或退出码非 0 时，桥接脚本直接透传退出码，避免吞掉错误上下文。
   */
  const forwardedArgs = process.argv.slice(2);
  const proc = Bun.spawn(
    ['bun', '-e', "import '@insnh-gd/meristem-tooling/cli'", 'tooling', ...forwardedArgs],
    {
      env: process.env,
      stdout: 'inherit',
      stderr: 'inherit',
      stdin: 'inherit',
    },
  );
  const exitCode = await proc.exited;
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
};

runToolingBridge();

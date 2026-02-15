#!/usr/bin/env bun

import { startApp } from '../index';
import { ensureMeristemHomeLayout, resolveMeristemHome } from '../runtime/paths';
import {
  doctorPlugins,
  listAvailablePlugins,
  listInstalledPlugins,
  refreshPluginRegistry,
  syncPlugins,
  syncRequiredLockedPlugins,
  updatePlugins,
} from '../runtime/plugin-manager';

class CliUsageError extends Error {
  readonly exitCode: number;

  constructor(message: string, exitCode = 2) {
    super(message);
    this.name = 'CliUsageError';
    this.exitCode = exitCode;
  }
}

type ParsedGlobal = Readonly<{
  home?: string;
  command: readonly string[];
}>;

const printHelp = (): void => {
  console.log(`
Meristem Core CLI

Usage:
  meristem-core [--home <path>] core start
  meristem-core [--home <path>] serve
  meristem-core [--home <path>] plugin <action> [...]

Pacman-like plugin commands:
  meristem-core [--home <path>] -Sy
    refresh plugin registry cache

  meristem-core [--home <path>] -Ss [keyword]
    list/search available plugins from registry

  meristem-core [--home <path>] -S <pluginId> [--ref <git-ref>]
    install/sync one plugin

  meristem-core [--home <path>] -S --required
    sync required plugins (enabled_by_default=true)

  meristem-core [--home <path>] -Su
    update all installed plugins

  meristem-core [--home <path>] -Syu
    refresh registry then update all installed plugins

  meristem-core [--home <path>] -Q
    list installed plugins (from lock file)

  meristem-core [--home <path>] -Qk
    doctor check installed plugins
`);
};

const parseGlobal = (argv: readonly string[]): ParsedGlobal => {
  const args = [...argv];
  let home: string | undefined;

  while (args.length > 0) {
    if (args[0] !== '--home') {
      break;
    }
    args.shift();
    const homeValue = args.shift();
    if (!homeValue || homeValue.trim().length === 0) {
      throw new CliUsageError('--home requires a non-empty path');
    }
    home = homeValue.trim();
  }

  return { home, command: args };
};

const parseFlagValue = (
  args: string[],
  flag: string,
): string | undefined => {
  const index = args.indexOf(flag);
  if (index < 0) {
    return undefined;
  }

  const value = args[index + 1];
  if (!value || value.startsWith('--')) {
    throw new CliUsageError(`${flag} requires a value`);
  }

  args.splice(index, 2);
  return value;
};

const parseBooleanFlag = (args: string[], flag: string): boolean => {
  const index = args.indexOf(flag);
  if (index < 0) {
    return false;
  }
  args.splice(index, 1);
  return true;
};

const takePositionals = (args: string[]): string[] => {
  const positionals: string[] = [];
  while (args.length > 0) {
    const token = args[0];
    if (!token || token.startsWith('-')) {
      break;
    }
    positionals.push(token);
    args.shift();
  }
  return positionals;
};

const assertNoUnknownArgs = (args: readonly string[]): void => {
  if (args.length > 0) {
    throw new CliUsageError(`unknown arguments: ${args.join(' ')}`);
  }
};

const runPluginCommand = async (
  home: string | undefined,
  args: readonly string[],
): Promise<void> => {
  const [action, ...rest] = args;
  const actionArgs = [...rest];
  const registryUrl = parseFlagValue(actionArgs, '--registry-url');
  const pluginId = parseFlagValue(actionArgs, '--plugin');
  const ref = parseFlagValue(actionArgs, '--ref');
  const required = parseBooleanFlag(actionArgs, '--required');
  const all = parseBooleanFlag(actionArgs, '--all');
  const listAvailable = parseBooleanFlag(actionArgs, '--available');
  const listInstalled = parseBooleanFlag(actionArgs, '--installed');
  assertNoUnknownArgs(actionArgs);

  if (action === 'refresh') {
    const result = await refreshPluginRegistry({ home, registryUrl });
    console.log(`[plugin:refresh] source=${result.source}`);
    console.log(`[plugin:refresh] cache=${result.cachePath}`);
    console.log(`[plugin:refresh] count=${result.pluginCount}`);
    return;
  }

  if (action === 'list') {
    if (listAvailable === listInstalled) {
      throw new CliUsageError('plugin list requires exactly one of --available or --installed');
    }
    if (listAvailable) {
      const plugins = await listAvailablePlugins({ home, registryUrl });
      console.log(JSON.stringify(plugins, null, 2));
      return;
    }
    const installed = listInstalledPlugins({ home });
    console.log(JSON.stringify(installed, null, 2));
    return;
  }

  if (action === 'sync') {
    const synced = await syncPlugins({
      home,
      registryUrl,
      pluginId,
      ref,
      requiredOnly: required,
    });
    console.log(JSON.stringify(synced, null, 2));
    return;
  }

  if (action === 'update') {
    const updated = await updatePlugins({
      home,
      registryUrl,
      pluginId,
      all,
    });
    console.log(JSON.stringify(updated, null, 2));
    return;
  }

  if (action === 'doctor') {
    const report = doctorPlugins({ home });
    console.log(JSON.stringify(report, null, 2));
    if (!report.ok) {
      throw new Error(`plugin doctor found ${report.issues.length} issue(s)`);
    }
    return;
  }

  throw new CliUsageError(`unknown plugin action: ${action ?? '(missing)'}`);
};

const runCoreCommand = async (
  home: string | undefined,
  args: readonly string[],
): Promise<void> => {
  const [action, ...rest] = args;
  assertNoUnknownArgs(rest);
  if (action !== 'start') {
    throw new CliUsageError(`unknown core action: ${action ?? '(missing)'}`);
  }

  const homeResult = resolveMeristemHome(home);
  ensureMeristemHomeLayout(homeResult.home);
  process.env.MERISTEM_RUNTIME_MODE = 'production';
  await syncRequiredLockedPlugins({ home: homeResult.home });
  await startApp({ homePath: homeResult.home, runtimeMode: 'production' });
};

const runServeCommand = async (home: string | undefined): Promise<void> => {
  await runCoreCommand(home, ['start']);
};

/**
 * 逻辑块：Pacman 风格命令适配层。
 * - 目的：提供 `-S/-Ss/-Sy/-Su/-Q/-Qk` 操作语义，降低插件管理心智负担。
 * - 原因：生产侧希望统一为包管理器式短命令入口，同时保持现有长命令兼容。
 * - 失败路径：未知操作或参数冲突直接返回 usage error（exit 2），避免隐式执行错误动作。
 */
const runPacmanCommand = async (
  home: string | undefined,
  args: readonly string[],
): Promise<void> => {
  const [operation, ...restInput] = args;
  if (!operation) {
    throw new CliUsageError('missing operation');
  }

  const rest = [...restInput];
  const registryUrl = parseFlagValue(rest, '--registry-url');
  const ref = parseFlagValue(rest, '--ref');
  const required = parseBooleanFlag(rest, '--required');
  const positionals = takePositionals(rest);
  assertNoUnknownArgs(rest);

  if (operation === '-Sy') {
    const result = await refreshPluginRegistry({ home, registryUrl });
    console.log(`[registry] source=${result.source}`);
    console.log(`[registry] cache=${result.cachePath}`);
    console.log(`[registry] count=${result.pluginCount}`);
    return;
  }

  if (operation === '-Ss') {
    if (positionals.length > 1) {
      throw new CliUsageError('-Ss accepts at most one keyword');
    }
    const keyword = positionals[0]?.toLowerCase();
    const plugins = await listAvailablePlugins({ home, registryUrl });
    const filtered = keyword
      ? plugins.filter((plugin) =>
        plugin.id.toLowerCase().includes(keyword)
        || plugin.name.toLowerCase().includes(keyword))
      : plugins;
    console.log(JSON.stringify(filtered, null, 2));
    return;
  }

  if (operation === '-S') {
    if (required && positionals.length > 0) {
      throw new CliUsageError('-S --required cannot be combined with plugin id');
    }
    if (!required && positionals.length !== 1) {
      throw new CliUsageError('-S requires exactly one plugin id, or use --required');
    }
    const synced = await syncPlugins({
      home,
      registryUrl,
      pluginId: required ? undefined : positionals[0],
      ref,
      requiredOnly: required,
    });
    console.log(JSON.stringify(synced, null, 2));
    return;
  }

  if (operation === '-Su') {
    if (positionals.length > 0) {
      throw new CliUsageError('-Su does not accept plugin id, use -S <pluginId> for single plugin sync');
    }
    const updated = await updatePlugins({
      home,
      registryUrl,
      all: true,
    });
    console.log(JSON.stringify(updated, null, 2));
    return;
  }

  if (operation === '-Syu') {
    if (positionals.length > 0) {
      throw new CliUsageError('-Syu does not accept plugin id');
    }
    await refreshPluginRegistry({ home, registryUrl });
    const updated = await updatePlugins({
      home,
      registryUrl,
      all: true,
    });
    console.log(JSON.stringify(updated, null, 2));
    return;
  }

  if (operation === '-Q') {
    if (positionals.length > 0) {
      throw new CliUsageError('-Q does not accept positional arguments');
    }
    const installed = listInstalledPlugins({ home });
    console.log(JSON.stringify(installed, null, 2));
    return;
  }

  if (operation === '-Qk') {
    if (positionals.length > 0) {
      throw new CliUsageError('-Qk does not accept positional arguments');
    }
    const report = doctorPlugins({ home });
    console.log(JSON.stringify(report, null, 2));
    if (!report.ok) {
      throw new Error(`plugin doctor found ${report.issues.length} issue(s)`);
    }
    return;
  }

  throw new CliUsageError(`unknown pacman operation: ${operation}`);
};

const run = async (): Promise<void> => {
  const parsed = parseGlobal(process.argv.slice(2));
  const [domain, ...rest] = parsed.command;
  if (!domain || domain === 'help' || domain === '--help' || domain === '-h') {
    printHelp();
    return;
  }

  if (domain === 'plugin') {
    await runPluginCommand(parsed.home, rest);
    return;
  }

  if (domain === 'serve') {
    await runServeCommand(parsed.home);
    return;
  }

  if (domain.startsWith('-')) {
    await runPacmanCommand(parsed.home, [domain, ...rest]);
    return;
  }

  if (domain === 'core') {
    await runCoreCommand(parsed.home, rest);
    return;
  }

  throw new CliUsageError(`unknown domain: ${domain}`);
};

run().catch((error: unknown) => {
  if (error instanceof CliUsageError) {
    console.error(`[meristem-core] ${error.message}`);
    process.exit(error.exitCode);
    return;
  }

  const message = error instanceof Error ? error.message : String(error);
  console.error(`[meristem-core] failed: ${message}`);
  process.exit(1);
});

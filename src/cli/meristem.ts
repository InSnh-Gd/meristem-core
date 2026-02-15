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
  meristem [--home <path>] core start
  meristem [--home <path>] plugin refresh [--registry-url <url-or-file>]
  meristem [--home <path>] plugin list --available|--installed [--registry-url <url-or-file>]
  meristem [--home <path>] plugin sync [--plugin <id>] [--ref <git-ref>] [--required]
  meristem [--home <path>] plugin update --all|--plugin <id> [--registry-url <url-or-file>]
  meristem [--home <path>] plugin doctor
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

  if (domain === 'core') {
    await runCoreCommand(parsed.home, rest);
    return;
  }

  throw new CliUsageError(`unknown domain: ${domain}`);
};

run().catch((error: unknown) => {
  if (error instanceof CliUsageError) {
    console.error(`[meristem] ${error.message}`);
    process.exit(error.exitCode);
    return;
  }

  const message = error instanceof Error ? error.message : String(error);
  console.error(`[meristem] failed: ${message}`);
  process.exit(1);
});

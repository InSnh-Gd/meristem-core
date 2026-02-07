import { randomBytes } from 'crypto';
import { getJwtRotationGraceSeconds, getJwtSignSecret, getJwtVerifySecrets } from '../config';
import {
  createRotatedJwtState,
  isGracePeriodElapsed,
  readJwtRotationState,
  writeJwtRotationState,
} from '../config/jwt-rotation-store';

type CliDeps = {
  nowMs?: () => number;
  randomSecret?: () => string;
  stdout?: (message: string) => void;
  stderr?: (message: string) => void;
};

type ParsedArgs = {
  command: 'rotate' | 'prune' | 'show';
  storePath?: string;
  graceSeconds?: number;
  force: boolean;
};

const DEFAULT_STORE_PATH = 'data/core/jwt-rotation.json';

const parseArgs = (argv: readonly string[]): ParsedArgs | null => {
  const [commandRaw, ...rest] = argv;
  if (commandRaw !== 'rotate' && commandRaw !== 'prune' && commandRaw !== 'show') {
    return null;
  }

  const parsed: ParsedArgs = {
    command: commandRaw,
    force: false,
  };

  for (let i = 0; i < rest.length; i += 1) {
    const token = rest[i];
    if (token === '--store') {
      parsed.storePath = rest[i + 1];
      i += 1;
      continue;
    }

    if (token === '--grace-seconds') {
      const rawValue = rest[i + 1];
      const parsedValue = Number.parseInt(rawValue ?? '', 10);
      if (Number.isFinite(parsedValue) && parsedValue > 0) {
        parsed.graceSeconds = parsedValue;
      }
      i += 1;
      continue;
    }

    if (token === '--force') {
      parsed.force = true;
    }
  }

  return parsed;
};

const usage = [
  'Usage:',
  '  bun run src/cli/jwt-rotate.ts rotate [--store <path>] [--grace-seconds <seconds>]',
  '  bun run src/cli/jwt-rotate.ts prune  [--store <path>] [--force]',
  '  bun run src/cli/jwt-rotate.ts show   [--store <path>]',
].join('\n');

const defaultRandomSecret = (): string => randomBytes(32).toString('hex');

export const runJwtRotationCli = async (
  argv: readonly string[],
  deps: CliDeps = {},
): Promise<number> => {
  const parsed = parseArgs(argv);
  const stdout = deps.stdout ?? ((message: string) => console.log(message));
  const stderr = deps.stderr ?? ((message: string) => console.error(message));
  const nowMs = deps.nowMs ?? (() => Date.now());
  const randomSecret = deps.randomSecret ?? defaultRandomSecret;

  if (!parsed) {
    stderr(usage);
    return 1;
  }

  const storePath = parsed.storePath ?? process.env.MERISTEM_SECURITY_JWT_ROTATION_STORE_PATH ?? DEFAULT_STORE_PATH;

  if (parsed.command === 'show') {
    const state = await readJwtRotationState(storePath);
    if (!state) {
      stdout(`Rotation store not found: ${storePath}`);
      return 0;
    }
    stdout(JSON.stringify(state, null, 2));
    return 0;
  }

  if (parsed.command === 'rotate') {
    const currentSignSecret = getJwtSignSecret();
    if (!currentSignSecret) {
      stderr('Current sign secret is empty, aborting rotation.');
      return 1;
    }

    const nextSignSecret = randomSecret();
    const graceSeconds = parsed.graceSeconds ?? getJwtRotationGraceSeconds();
    const state = createRotatedJwtState(
      nextSignSecret,
      currentSignSecret,
      getJwtVerifySecrets(),
      graceSeconds,
      nowMs(),
    );

    await writeJwtRotationState(storePath, state);
    stdout(`JWT rotation updated: ${storePath}`);
    stdout('Restart meristem-core to apply new signing secret.');
    return 0;
  }

  const state = await readJwtRotationState(storePath);
  if (!state) {
    stderr(`Rotation store not found: ${storePath}`);
    return 1;
  }

  if (!parsed.force && !isGracePeriodElapsed(state, nowMs())) {
    stderr('Grace period has not elapsed. Use --force to prune immediately.');
    return 1;
  }

  await writeJwtRotationState(storePath, {
    ...state,
    verify_secrets: [state.current_sign_secret],
  });
  stdout(`JWT verify secret list pruned: ${storePath}`);
  return 0;
};

if (import.meta.main) {
  const args = process.argv.slice(2);
  const exitCode = await runJwtRotationCli(args);
  process.exit(exitCode);
}

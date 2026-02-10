type BunWithWhich = typeof Bun & {
  which?: (command: string) => string | null;
};

const bun = Bun as BunWithWhich;
const tsgoPath = typeof bun.which === 'function' ? bun.which('tsgo') : null;

if (!tsgoPath) {
  console.warn('[typecheck:next] tsgo not found, skipping preview typecheck');
  process.exit(0);
}

const run = Bun.spawnSync({
  cmd: ['tsgo', '--noEmit', '-p', 'tsconfig.json'],
  stdout: 'inherit',
  stderr: 'inherit',
});

if (run.exitCode !== 0) {
  process.exit(run.exitCode);
}


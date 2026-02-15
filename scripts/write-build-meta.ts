import { mkdirSync, writeFileSync } from 'fs';
import { dirname, join, resolve } from 'path';

const rootDir = resolve(import.meta.dir, '..');
const outputPath = join(rootDir, 'src', 'generated', 'build-meta.ts');
const buildHome = (process.env.MERISTEM_BUILD_HOME ?? rootDir).trim();

const content = [
  '/**',
  ' * 该文件由 scripts/write-build-meta.ts 自动生成。',
  ' * 构建二进制时会把默认 MERISTEM_HOME 固定为编译机上的仓库路径。',
  ' */',
  `export const BUILD_DEFAULT_HOME = ${JSON.stringify(buildHome)};`,
  '',
].join('\n');

mkdirSync(dirname(outputPath), { recursive: true });
writeFileSync(outputPath, content, 'utf-8');
console.log(`[build-meta] wrote ${outputPath}`);
console.log(`[build-meta] home=${buildHome}`);

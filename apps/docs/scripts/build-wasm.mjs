// Build the playground's WebAssembly engine (crates/ffkv-wasm) and copy it to
// public/playground/ffkv.wasm, where Astro serves and publishes it.
import { spawnSync } from 'node:child_process';
import { copyFile, mkdir, stat } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const destination = fileURLToPath(new URL('../public/playground/ffkv.wasm', import.meta.url));
const args = ['build', '--release', '--target', 'wasm32-wasip1', '-p', 'ffkv-wasm', '--message-format=json-render-diagnostics'];

const build = spawnSync('cargo', args, { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'], maxBuffer: 256 * 1024 * 1024 });
if (build.error) throw new Error(`Could not run cargo (the playground needs Rust with the wasm32-wasip1 target: rustup target add wasm32-wasip1): ${build.error.message}`);
if (build.status !== 0) throw new Error(`cargo ${args.join(' ')} failed with exit code ${build.status}`);

// Ask Cargo where it wrote the artifact instead of assuming a target directory.
const artifact = build.stdout.split('\n')
  .filter((line) => line.startsWith('{'))
  .map((line) => JSON.parse(line))
  .filter((message) => message.reason === 'compiler-artifact' && message.target?.name === 'ffkv_wasm')
  .flatMap((message) => message.filenames)
  .find((file) => file.endsWith('.wasm'));
if (!artifact) throw new Error('cargo did not report an ffkv_wasm .wasm artifact');

await mkdir(path.dirname(destination), { recursive: true });
await copyFile(artifact, destination);
const { size } = await stat(destination);
console.log(`Playground engine: ${path.relative(root, artifact)} -> ${path.relative(root, destination)} (${(size / 1024 / 1024).toFixed(2)} MiB)`);

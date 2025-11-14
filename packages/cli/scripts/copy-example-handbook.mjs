import { existsSync } from 'fs';
import { cp, mkdir, rm } from 'fs/promises';
import { dirname, join, resolve } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, '..', '..');
const cliDir = join(repoRoot, 'cli');
const distDir = join(cliDir, 'dist');
const dest = join(distDir, 'example-handbook');

const candidateSources = [
  // Legacy location (pre-refactor)
  join(repoRoot, 'src', 'acme-analytics-server', 'onemcp-handbook'),
  // Current location bundled with the Java resources
  join(repoRoot, 'src', 'onemcp', 'src', 'main', 'resources', 'acme-handbook'),
  // Already-built artefact (when running within dist/)
  join(cliDir, 'dist', 'example-handbook'),
];

async function copyExampleHandbook() {
  const source = candidateSources.find((candidate) => existsSync(candidate));

  if (!source) {
    console.warn('[copy-example-handbook] No example handbook found. Skipping copy step.');
    return;
  }

  await mkdir(distDir, { recursive: true });
  await rm(dest, { recursive: true, force: true });
  await cp(source, dest, { recursive: true });

  console.log(`[copy-example-handbook] Copied example handbook from ${source} -> ${dest}`);
}

copyExampleHandbook().catch((error) => {
  console.error('[copy-example-handbook] Failed to copy example handbook:', error);
  process.exitCode = 1;
});


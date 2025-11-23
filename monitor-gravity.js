'use strict';

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const { exec } = require('child_process');
const util = require('util');
const execp = util.promisify(exec);

/**
 * Gravity total.json monitor
 * - On start:
 *    * git pull (if repo is a git repo and network available)
 *    * read gravity/total.json and emit reddit.json, x.json, yt.json
 * - Then loop 24x7:
 *    * git pull on an interval
 *    * if gravity/total.json content hash changes -> regenerate the 3 files
 *
 * Env vars:
 *   REPO_DIR     (default: process.cwd())
 *   GRAVITY_DIR  (default: path.join(REPO_DIR, "gravity"))
 *   INTERVAL_MS  (default: 60000 ms)
 *   NO_GIT       (if set to "1", skip git operations)
 */

const REPO_DIR = process.env.REPO_DIR ? path.resolve(process.env.REPO_DIR) : process.cwd();
const GRAVITY_DIR = process.env.GRAVITY_DIR
  ? path.resolve(process.env.GRAVITY_DIR)
  : path.join(REPO_DIR, 'gravity');
const TOTAL_PATH = path.join(GRAVITY_DIR, 'total.json');

const OUT_MAP = {
  reddit: path.join(GRAVITY_DIR, 'reddit.json'),
  x: path.join(GRAVITY_DIR, 'x.json'),
  youtube: path.join(GRAVITY_DIR, 'yt.json'),
};

const INTERVAL_MS = Number(process.env.INTERVAL_MS || 60000);
const NO_GIT = process.env.NO_GIT === '1';

function ts() {
  return new Date().toISOString();
}

function md5(buf) {
  return crypto.createHash('md5').update(buf).digest('hex');
}

async function fileExists(p) {
  try {
    await fsp.access(p);
    return true;
  } catch {
    return false;
  }
}

async function ensureDir(p) {
  await fsp.mkdir(p, { recursive: true });
}

async function gitPull(repoDir) {
  if (NO_GIT) {
    log('NO_GIT=1 set, skipping git pull');
    return { changed: false, stdout: '', stderr: '' };
  }
  // Only attempt if it looks like a git repo
  if (!fs.existsSync(path.join(repoDir, '.git'))) {
    log(`No .git directory in ${repoDir}, skipping git pull`);
    return { changed: false, stdout: '', stderr: '' };
  }
  try {
    // Fetch and fast-forward pull
    const { stdout: fStd } = await execp('git fetch --all --prune', { cwd: repoDir });
    const before = (await execp('git rev-parse HEAD', { cwd: repoDir })).stdout.trim();
    const { stdout: pStd } = await execp('git pull --ff-only', { cwd: repoDir });
    const after = (await execp('git rev-parse HEAD', { cwd: repoDir })).stdout.trim();
    const changed = before !== after;
    if (changed) {
      log(`Git updated: ${before} -> ${after}`);
    }
    return { changed, stdout: `${fStd}\n${pStd}`, stderr: '' };
  } catch (err) {
    log(`git pull failed: ${err.message}`);
    return { changed: false, stdout: '', stderr: err.stderr || '' };
  }
}

function log(msg) {
  console.log(`[${ts()}] ${msg}`);
}

async function readTotalJson() {
  const buf = await fsp.readFile(TOTAL_PATH);
  const hash = md5(buf);
  let data;
  try {
    data = JSON.parse(buf.toString('utf8'));
  } catch (e) {
    throw new Error(`Failed to parse JSON at ${TOTAL_PATH}: ${e.message}`);
  }
  if (!Array.isArray(data)) {
    throw new Error(`Expected ${TOTAL_PATH} to be a JSON array`);
  }
  return { data, hash };
}

function filterByPlatform(items, platform) {
  return items.filter((it) => {
    const p = it && it.params && it.params.platform;
    return typeof p === 'string' && p.toLowerCase() === platform;
  });
}

async function writeJsonIfChanged(outPath, obj) {
  const tmp = `${outPath}.tmp`;
  const content = JSON.stringify(obj, null, 2);
  let current = null;
  if (await fileExists(outPath)) {
    current = await fsp.readFile(outPath, 'utf8');
  }
  if (current === content) {
    // No change
    return false;
  }
  await fsp.writeFile(tmp, content);
  await fsp.rename(tmp, outPath);
  return true;
}

async function regenerateOutputs(items) {
  const byReddit = filterByPlatform(items, 'reddit');
  const byX = filterByPlatform(items, 'x');
  const byYouTube = filterByPlatform(items, 'youtube');

  const changed = {
    reddit: await writeJsonIfChanged(OUT_MAP.reddit, byReddit),
    x: await writeJsonIfChanged(OUT_MAP.x, byX),
    youtube: await writeJsonIfChanged(OUT_MAP.youtube, byYouTube),
  };

  const changedKeys = Object.entries(changed)
    .filter(([, v]) => v)
    .map(([k]) => k);
  if (changedKeys.length) {
    log(`Wrote updated outputs: ${changedKeys.join(', ')}`);
  } else {
    log('Outputs are up to date (no changes)');
  }
}

async function main() {
  log(`Starting gravity monitor
  REPO_DIR=${REPO_DIR}
  GRAVITY_DIR=${GRAVITY_DIR}
  TOTAL_PATH=${TOTAL_PATH}
  INTERVAL_MS=${INTERVAL_MS}
  NO_GIT=${NO_GIT}`);

  await ensureDir(GRAVITY_DIR);

  // Initial pull + generate
  await gitPull(REPO_DIR);
  if (!(await fileExists(TOTAL_PATH))) {
    log(`ERROR: ${TOTAL_PATH} does not exist. Exiting.`);
    process.exit(2);
  }

  let prevHash = null;

  try {
    const { data, hash } = await readTotalJson();
    prevHash = hash;
    log(`Initial read: total.json hash=${hash}`);
    await regenerateOutputs(data);
  } catch (e) {
    log(`Initial processing failed: ${e.message}`);
  }

  // 24/7 loop
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      await gitPull(REPO_DIR);

      const { data, hash } = await readTotalJson();

      if (hash !== prevHash) {
        log(`Change detected in total.json: ${prevHash || '(none)'} -> ${hash}`);
        await regenerateOutputs(data);
        prevHash = hash;
      } else {
        log('No change detected in total.json');
      }
    } catch (e) {
      log(`Loop error: ${e.message}`);
    }

    await new Promise((res) => setTimeout(res, INTERVAL_MS));
  }
}

process.on('SIGINT', () => {
  log('Received SIGINT, exiting');
  process.exit(0);
});
process.on('SIGTERM', () => {
  log('Received SIGTERM, exiting');
  process.exit(0);
});

main().catch((e) => {
  log(`Fatal error: ${e.message}`);
  process.exit(1);
});

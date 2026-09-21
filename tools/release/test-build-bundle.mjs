#!/usr/bin/env node
// Exercise the real release builder in an isolated Git checkout with synthetic inputs.
import assert from 'node:assert/strict';
import {cp, mkdir, mkdtemp, rm, symlink, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const sandbox = await mkdtemp(join(tmpdir(), 'collector-release-test-'));
function run(program, args, options = {}) {
  const result = spawnSync(program, args, {cwd: sandbox, encoding: 'utf8', ...options});
  assert.equal(result.status, 0, `${program} failed: ${result.stderr}`);
  return result.stdout;
}
const paths = run('git', ['ls-files', '-z', '--', 'deploy', 'tools/ops', 'tools/release/build-bundle.mjs'], {cwd: root}).split('\0').filter(Boolean);
try {
  for (const path of paths) {
    await mkdir(dirname(join(sandbox, path)), {recursive: true});
    await cp(join(root, path), join(sandbox, path));
  }
  run('git', ['init', '--quiet']);
  run('git', ['add', '--', 'deploy', 'tools']);
  await writeFile(join(sandbox, '.gitignore'), '*.env\n*.pem\ncookies*.json\n');
  const sidecars = ['deploy/local.env', 'deploy/.env.production', 'deploy/cookies.json', 'deploy/server.pem', 'deploy/migrations/999_local.sql', 'deploy/initdb/local.sql', 'tools/ops/local-note.txt'];
  for (const path of sidecars) await writeFile(join(sandbox, path), 'synthetic-local-only-marker\n');
  const env = {...process.env, SOURCE_SHA: '1'.repeat(40)};
  for (const role of ['POSTGRES', 'REDIS', 'DISCOVER', 'COORDINATOR', 'WORKER', 'QUERY', 'COOKIE_AUTH']) {
    env[`IMAGE_${role}`] = `registry.example/fixture/${role.toLowerCase()}@sha256:${'a'.repeat(64)}`;
  }
  run(process.execPath, ['tools/release/build-bundle.mjs'], {env});
  const archive = 'dist/release/release-bundle.tar.gz';
  const contents = new Set(run('tar', ['-tzf', archive]).trim().split('\n').map(p => p.replace(/^\.\//, '')));
  assert(contents.has('deploy/compose.production.yaml'));
  assert(contents.has('deploy/.env.example'));
  for (const path of sidecars) assert(!contents.has(path), `local file leaked: ${path}`);
  const manifest = JSON.parse(run('tar', ['-xOzf', archive, './manifest.build.json']));
  assert.equal(manifest.profile, 'soop-single-channel');
  assert.equal(Object.keys(manifest.images).length, 7);
  assert(!manifest.migrations.some(x => x.path.endsWith('999_local.sql')));
  assert(!manifest.runtimeFiles.some(x => x.path.endsWith('local.sql')));
  assert(!run('tar', ['-xOzf', archive]).includes('synthetic-local-only-marker'));
  run('sha256sum', ['--check', 'release-bundle.tar.gz.sha256'], {cwd: join(sandbox, 'dist/release')});
  await writeFile(join(sandbox, 'outside.txt'), 'synthetic-outside-marker\n');
  await symlink('../outside.txt', join(sandbox, 'deploy/linked.txt'));
  run('git', ['add', '--', 'deploy/linked.txt']);
  const rejected = spawnSync(process.execPath, ['tools/release/build-bundle.mjs'], {cwd: sandbox, env, encoding: 'utf8'});
  assert.notEqual(rejected.status, 0, 'tracked symlink was accepted');
  assert.match(rejected.stderr, /not a regular file/);
  console.log('Release archive checks passed: local sidecars/SQL excluded, tracked inputs retained, symlinks rejected.');
} finally {
  await rm(sandbox, {recursive: true, force: true});
}

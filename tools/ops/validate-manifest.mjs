#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFile, readdir, realpath, stat } from 'node:fs/promises';
import { dirname, resolve, sep } from 'node:path';

const fail = message => { throw new Error(message); };
const sha256 = value => createHash('sha256').update(value).digest('hex');
const [manifestPath] = process.argv.slice(2);
if (!manifestPath) fail('usage: validate-manifest.mjs MANIFEST');
const absolute = await realpath(manifestPath);
const root = dirname(absolute);
const manifest = JSON.parse(await readFile(absolute, 'utf8'));
const exact = (value, pattern, label) => typeof value === 'string' && pattern.test(value) || fail(`invalid ${label}`);
if (manifest.schemaVersion !== 1 || manifest.product !== 'rogi-collector' || manifest.profile !== 'feedback') fail('unsupported product/profile/schemaVersion');
exact(manifest.sourceSha, /^[0-9a-f]{40}$/, 'sourceSha');
exact(manifest.releaseId, /^[A-Za-z0-9._-]{1,96}$/, 'releaseId');
exact(manifest.contractVersion, /^v[0-9]+(?:\.[0-9]+){0,2}$/, 'contractVersion');
exact(manifest.composeSha256, /^[0-9a-f]{64}$/, 'composeSha256');
const initdbNames = (await readdir(resolve(root, 'deploy/initdb'), {withFileTypes: true})).filter(item => item.isFile()).map(item => `deploy/initdb/${item.name}`).sort();
const requiredRuntimePaths = ['deploy/run-migrations.sh','deploy/install-runtime.sh','deploy/systemd/rogi-collector-host-ready.service','deploy/systemd/rogi-collector-migrate.service','deploy/systemd/rogi-collector-role@.service','deploy/systemd/rogi-collector-update.service','deploy/systemd/rogi-collector-update.timer','deploy/systemd/rogi-collector-backup.service','deploy/systemd/rogi-collector-backup.timer','deploy/systemd/rogi-collector.target',...['deploy.sh','status.sh','prepare-host.sh','validate-manifest.mjs','render-runtime-env.mjs','fetch-release.py','production-status.py','backup-postgres.sh','load-secrets-aws.py','upload-backup-s3.py','validate-runtime-secrets.sh','load-registry-auth.py'].map(name=>`tools/ops/${name}`),...initdbNames];
const requiredRuntimeFiles = new Set(requiredRuntimePaths);
if (!Array.isArray(manifest.runtimeFiles) || manifest.runtimeFiles.length !== requiredRuntimeFiles.size) fail('runtimeFiles must contain the exact executable allowlist');
for (const item of manifest.runtimeFiles) {
  if (!requiredRuntimeFiles.delete(item?.path)) fail(`unexpected or duplicate runtime file: ${item?.path}`);
  exact(item.sha256, /^[0-9a-f]{64}$/, 'runtime file sha256');
  const file = resolve(root, item.path);
  if (!file.startsWith(root + sep) || !(await stat(file)).isFile()) fail(`missing runtime file ${item.path}`);
  if (sha256(await readFile(file)) !== item.sha256) fail(`runtime file checksum mismatch: ${item.path}`);
}
if (requiredRuntimeFiles.size) fail('runtimeFiles allowlist is incomplete');
const requiredImages = ['postgres', 'redis', 'discover', 'coordinator', 'worker', 'query'];
for (const role of requiredImages) exact(manifest.images?.[role], /^[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9._-]+)?@sha256:[0-9a-f]{64}$/, `images.${role}`);
if (!Array.isArray(manifest.migrations) || manifest.migrations.length < 1) fail('migrations must be non-empty');
const diskMigrationPaths = (await readdir(resolve(root, 'deploy/migrations'), {withFileTypes: true})).filter(item => item.isFile() && item.name.endsWith('.sql')).map(item => `deploy/migrations/${item.name}`).sort();
const declaredMigrationPaths = manifest.migrations.map(item => item?.path);
if (JSON.stringify(declaredMigrationPaths) !== JSON.stringify(diskMigrationPaths)) fail('migrations must exactly match the sorted on-disk SQL list');
for (const item of manifest.migrations) {
  exact(item.path, /^deploy\/migrations\/[A-Za-z0-9._-]+\.sql$/, 'migration path');
  exact(item.sha256, /^[0-9a-f]{64}$/, 'migration sha256');
  const file = resolve(root, item.path); if (!file.startsWith(root + sep)) fail('migration escapes release root');
  if (!(await stat(file)).isFile()) fail(`missing migration ${item.path}`);
  if (sha256(await readFile(file)) !== item.sha256) fail(`migration checksum mismatch: ${item.path}`);
}
const compose = resolve(root, 'deploy/compose.production.yaml');
if (sha256(await readFile(compose)) !== manifest.composeSha256) fail('compose checksum mismatch');
const runtime = manifest.runtimeNonSecret;
if (!runtime || runtime.composeProjectName !== 'rogi-collector' || runtime.dataRoot !== '/srv/rogi-collector' || runtime.secretsRoot !== '/run/rogi-collector' || runtime.releaseRoot !== '/opt/rogi-collector/app/current') fail('invalid runtimeNonSecret host paths');
const runtimeKeys = ['capabilities', 'composeProjectName', 'dataRoot', 'postgresAdminUser', 'postgresDatabase', 'postgresMigrateUser', 'releaseRoot', 'secretsRoot'];
if (JSON.stringify(Object.keys(runtime).sort()) !== JSON.stringify(runtimeKeys)) fail('unexpected runtimeNonSecret field');
exact(runtime.postgresDatabase, /^[a-z_][a-z0-9_]{0,62}$/, 'postgresDatabase');
exact(runtime.postgresAdminUser, /^[a-z_][a-z0-9_]{0,62}$/, 'postgresAdminUser');
exact(runtime.postgresMigrateUser, /^[a-z_][a-z0-9_]{0,62}$/, 'postgresMigrateUser');
if (Object.keys(runtime.capabilities ?? {}).length !== 1) fail('unexpected capabilities field');
if (runtime.capabilities?.grpc7443 !== 'unavailable-health-only') fail('feedback profile must declare grpc7443 unavailable-health-only');
process.stdout.write(JSON.stringify({ok: true, releaseId: manifest.releaseId, sourceSha: manifest.sourceSha}) + '\n');

#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
const manifest = JSON.parse(await readFile(process.argv[2], 'utf8'));
const q = value => `'${String(value).replaceAll("'", "'\\''")}'`;
const lines = {
  COMPOSE_PROJECT_NAME: manifest.runtimeNonSecret.composeProjectName,
  DATA_ROOT: manifest.runtimeNonSecret.dataRoot,
  SECRETS_ROOT: manifest.runtimeNonSecret.secretsRoot,
  RELEASE_ROOT: manifest.runtimeNonSecret.releaseRoot,
  POSTGRES_DB: manifest.runtimeNonSecret.postgresDatabase,
  POSTGRES_ADMIN_USER: manifest.runtimeNonSecret.postgresAdminUser,
  POSTGRES_MIGRATE_USER: manifest.runtimeNonSecret.postgresMigrateUser,
  POSTGRES_IMAGE: manifest.images.postgres, REDIS_IMAGE: manifest.images.redis,
  DISCOVER_IMAGE: manifest.images.discover, COORDINATOR_IMAGE: manifest.images.coordinator,
  WORKER_IMAGE: manifest.images.worker, QUERY_IMAGE: manifest.images.query,
  COOKIE_AUTH_IMAGE: manifest.images["cookie-auth"], QUERY_BIND_IP: manifest.runtimeNonSecret.queryBindIp,
};
for (const [key, value] of Object.entries(lines)) process.stdout.write(`${key}=${q(value)}\n`);

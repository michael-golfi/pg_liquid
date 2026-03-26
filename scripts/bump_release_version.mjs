#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const rootDir = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const bumpType = process.argv[2] ?? 'patch';
const validBumps = new Set(['major', 'minor', 'patch']);

if (!validBumps.has(bumpType)) {
  console.error(`unsupported bump type: ${bumpType}`);
  process.exit(1);
}

const readText = (relativePath) => fs.readFileSync(path.join(rootDir, relativePath), 'utf8');
const writeText = (relativePath, value) => fs.writeFileSync(path.join(rootDir, relativePath), value);

const metaPath = 'META.json';
const packagePath = 'package.json';
const packageLockPath = 'package-lock.json';
const controlPath = 'pg_liquid.control';
const changesPath = 'Changes.md';

const meta = JSON.parse(readText(metaPath));
const pkg = JSON.parse(readText(packagePath));
const packageLock = JSON.parse(readText(packageLockPath));
const control = readText(controlPath);

const versionMatch = control.match(/^default_version\s*=\s*'([^']+)'/m);
if (!versionMatch) {
  console.error('missing default_version in pg_liquid.control');
  process.exit(1);
}

const currentVersion = meta.version;
if (pkg.version !== currentVersion || versionMatch[1] !== currentVersion) {
  console.error(`version mismatch: META=${meta.version} package=${pkg.version} control=${versionMatch[1]}`);
  process.exit(1);
}

const currentParts = currentVersion.match(/^(\d+)\.(\d+)\.(\d+)$/);
if (!currentParts) {
  console.error(`unsupported semantic version: ${currentVersion}`);
  process.exit(1);
}

let [major, minor, patch] = currentParts.slice(1).map((value) => Number(value));
if (bumpType === 'major') {
  major += 1;
  minor = 0;
  patch = 0;
} else if (bumpType === 'minor') {
  minor += 1;
  patch = 0;
} else {
  patch += 1;
}

const nextVersion = `${major}.${minor}.${patch}`;

const replaceVersion = (input, from, to) => input.split(from).join(to);

meta.version = nextVersion;
meta.provides.pg_liquid.version = nextVersion;
meta.provides.pg_liquid.file = `sql/pg_liquid--${nextVersion}.sql`;
writeText(metaPath, `${JSON.stringify(meta, null, 2)}\n`);

pkg.version = nextVersion;
writeText(packagePath, `${JSON.stringify(pkg, null, 2)}\n`);

packageLock.version = nextVersion;
if (packageLock.packages?.['']) {
  packageLock.packages[''].version = nextVersion;
}
writeText(packageLockPath, `${JSON.stringify(packageLock, null, 2)}\n`);

writeText(controlPath, control.replace(
  /^default_version\s*=\s*'[^']+'/m,
  `default_version = '${nextVersion}'`,
));

const currentInstallPath = path.join(rootDir, 'sql', `pg_liquid--${currentVersion}.sql`);
const nextInstallPath = path.join(rootDir, 'sql', `pg_liquid--${nextVersion}.sql`);
if (!fs.existsSync(currentInstallPath)) {
  console.error(`missing install script for ${currentVersion}`);
  process.exit(1);
}

const currentInstallSql = readText(path.relative(rootDir, currentInstallPath));
writeText(
  path.relative(rootDir, nextInstallPath),
  replaceVersion(currentInstallSql, currentVersion, nextVersion),
);

const upgradePath = path.join(rootDir, 'sql', `pg_liquid--${currentVersion}--${nextVersion}.sql`);
const upgradeSql = `-- pg_liquid ${currentVersion} -> ${nextVersion}
-- Release version bump; no schema changes.
`;
writeText(path.relative(rootDir, upgradePath), upgradeSql);

const changes = readText(changesPath);
const releaseDate = new Date().toISOString().slice(0, 10);
const nextEntry = `## ${nextVersion}\n\n- automated ${bumpType} release on ${releaseDate}\n\n`;
const updatedChanges = changes.replace(/^# Changes\n\n/, `# Changes\n\n${nextEntry}`);
writeText(changesPath, updatedChanges);

console.log(nextVersion);

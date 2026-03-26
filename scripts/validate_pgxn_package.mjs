#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const workdir = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const metaPath = path.join(workdir, 'META.json');
const controlPath = path.join(workdir, 'pg_liquid.control');
const requiredFiles = [
  'README.md',
  'LICENSE',
  'Changes.md',
  'META.json',
  'pg_liquid.control',
];

for (const relativePath of requiredFiles) {
  const fullPath = path.join(workdir, relativePath);
  if (!fs.existsSync(fullPath)) {
    throw new Error(`missing required packaging file: ${relativePath}`);
  }
}

const meta = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
const control = fs.readFileSync(controlPath, 'utf8');
const topLevelAllowedKeys = new Set([
  'abstract',
  'description',
  'generated_by',
  'license',
  'maintainer',
  'meta-spec',
  'name',
  'no_index',
  'prereqs',
  'provides',
  'release_status',
  'resources',
  'tags',
  'version',
]);
const resourceAllowedKeys = new Set([
  'bugtracker',
  'homepage',
  'repository',
]);

const defaultVersionMatch = control.match(/^default_version\s*=\s*'([^']+)'/m);
if (!defaultVersionMatch) {
  throw new Error('pg_liquid.control is missing default_version');
}

for (const key of Object.keys(meta)) {
  if (!topLevelAllowedKeys.has(key) && !/^x_/i.test(key)) {
    throw new Error(`META.json contains unsupported top-level key for PGXN Meta Spec 1.0.0: ${key}`);
  }
}

if (meta.resources) {
  for (const key of Object.keys(meta.resources)) {
    if (!resourceAllowedKeys.has(key) && !/^x_/i.test(key)) {
      throw new Error(`META.json resources contains unsupported PGXN Meta Spec 1.0.0 key: ${key}`);
    }
  }
}

const defaultVersion = defaultVersionMatch[1];
if (meta.version !== defaultVersion) {
  throw new Error(`META.json version ${meta.version} does not match control default_version ${defaultVersion}`);
}

if (!meta.provides?.pg_liquid?.version || meta.provides.pg_liquid.version !== meta.version) {
  throw new Error('META.json provides.pg_liquid.version must match META.json version');
}

const providedFile = meta.provides?.pg_liquid?.file;
if (!providedFile) {
  throw new Error('META.json provides.pg_liquid.file is required');
}

const installScriptPath = path.join(workdir, providedFile);
if (!fs.existsSync(installScriptPath)) {
  throw new Error(`referenced install script does not exist: ${providedFile}`);
}

const providedDocfile = meta.provides?.pg_liquid?.docfile;
if (providedDocfile && !fs.existsSync(path.join(workdir, providedDocfile))) {
  throw new Error(`referenced docfile does not exist: ${providedDocfile}`);
}

if (!/^\d+\.\d+\.\d+$/.test(meta.version)) {
  throw new Error(`META.json version must be semver-compatible, got ${meta.version}`);
}

console.log(`PGXN package metadata OK for pg_liquid ${meta.version}`);

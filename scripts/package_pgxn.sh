#!/usr/bin/env bash
set -euo pipefail

WORKDIR="$(cd "$(dirname "$0")/.." && pwd)"
OUTDIR="$WORKDIR/release"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

VERSION="$(node -e 'const fs=require("fs"); const meta=JSON.parse(fs.readFileSync(process.argv[1], "utf8")); console.log(meta.version);' "$WORKDIR/META.json")"
PKGDIR="pg_liquid-$VERSION"
STAGE="$TMPDIR/$PKGDIR"
ARCHIVE="$OUTDIR/$PKGDIR.tar.gz"

mkdir -p "$STAGE" "$OUTDIR"

for path in \
  META.json \
  README.md \
  LICENSE \
  Changes.md \
  Makefile \
  pg_liquid.control \
  .gitignore \
  datalog \
  docs \
  expected \
  scripts \
  sql \
  src \
  tests \
  tsconfig.json \
  vitest.config.ts
do
  if [[ -e "$WORKDIR/$path" ]]; then
    cp -R "$WORKDIR/$path" "$STAGE/$path"
  fi
done

find "$STAGE" -name '.DS_Store' -exec rm -f {} +

tar -C "$TMPDIR" -czf "$ARCHIVE" "$PKGDIR"
printf 'created %s\n' "$ARCHIVE"

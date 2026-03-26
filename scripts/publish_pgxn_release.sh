#!/usr/bin/env bash
set -euo pipefail

ARCHIVE_PATH="${1:-}"
PGXN_UPLOAD_URL="${PGXN_UPLOAD_URL:-https://manager.pgxn.org/upload}"
PGXN_UPLOAD_FIELD="${PGXN_UPLOAD_FIELD:-archive}"

if [[ -z "$ARCHIVE_PATH" ]]; then
  echo "usage: $0 <archive-path>" >&2
  exit 1
fi

if [[ ! -f "$ARCHIVE_PATH" ]]; then
  echo "archive not found: $ARCHIVE_PATH" >&2
  exit 1
fi

: "${PGXN_USERNAME:?PGXN_USERNAME is required}"
: "${PGXN_PASSWORD:?PGXN_PASSWORD is required}"

curl \
  --fail \
  --silent \
  --show-error \
  --user "${PGXN_USERNAME}:${PGXN_PASSWORD}" \
  -F "${PGXN_UPLOAD_FIELD}=@${ARCHIVE_PATH}" \
  "$PGXN_UPLOAD_URL"

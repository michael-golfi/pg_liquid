#!/usr/bin/env bash
set -euo pipefail

PG_MAJOR="${PG_MAJOR:-16}"
PG_KEYRING="/usr/share/keyrings/postgresql.gpg"
PG_LIST="/etc/apt/sources.list.d/pgdg.list"
PG_BIN_DIR="/usr/lib/postgresql/${PG_MAJOR}/bin"

sudo apt-get update
sudo apt-get install -y curl ca-certificates gnupg lsb-release build-essential clang libicu-dev

if [[ ! -f "$PG_KEYRING" ]]; then
  curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o "$PG_KEYRING"
fi

if [[ ! -f "$PG_LIST" ]]; then
  echo "deb [signed-by=$PG_KEYRING] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee "$PG_LIST" >/dev/null
fi

sudo apt-get update
sudo apt-get install -y "postgresql-${PG_MAJOR}" "postgresql-server-dev-${PG_MAJOR}"

if [[ -n "${GITHUB_PATH:-}" ]]; then
  echo "$PG_BIN_DIR" >> "$GITHUB_PATH"
fi

cluster_exists=0
if sudo pg_lsclusters --no-header | awk -v major="$PG_MAJOR" '$1 == major && $2 == "main" { found = 1 } END { exit found ? 0 : 1 }'; then
  cluster_exists=1
fi

if [[ "$cluster_exists" == "1" ]]; then
  cluster_status="$(
    sudo pg_lsclusters --no-header \
      | awk -v major="$PG_MAJOR" '$1 == major && $2 == "main" { print $4; exit }'
  )"
  if [[ "$cluster_status" != "online" ]]; then
    sudo pg_ctlcluster "$PG_MAJOR" main start
  fi
else
  sudo pg_createcluster "$PG_MAJOR" main --start
fi

PG_PORT="$(
  sudo pg_lsclusters --no-header \
    | awk -v major="$PG_MAJOR" '$1 == major && $2 == "main" { print $3; exit }'
)"

if [[ -z "$PG_PORT" ]]; then
  echo "could not determine PostgreSQL ${PG_MAJOR} cluster port" >&2
  exit 1
fi

if [[ -n "${GITHUB_ENV:-}" ]]; then
  {
    echo "PG_CONFIG=${PG_BIN_DIR}/pg_config"
    echo "PGHOST=127.0.0.1"
    echo "PGPORT=${PG_PORT}"
    echo "PGUSER=postgres"
    echo "PGPASSWORD=postgres"
    echo "PGDATABASE=postgres"
  } >> "$GITHUB_ENV"
fi

sudo -u postgres "${PG_BIN_DIR}/psql" -p "$PG_PORT" -c "ALTER USER postgres PASSWORD 'postgres';"

#!/usr/bin/env bash
set -euo pipefail

# Override these with environment variables if the container or database changes.
CONTAINER="${CONTAINER:-hydromancer-timescaledb}"
DATABASE="${DATABASE:-hydromancer}"
DB_USER="${DB_USER:-postgres}"

if (( $# > 1 )); then
  echo "Usage: $0 [path/to/query.sql]" >&2
  exit 2
fi

if ! docker inspect --format '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -qx true; then
  echo "Error: Docker container '$CONTAINER' is not running." >&2
  exit 1
fi

if (( $# == 1 )); then
  SQL_FILE="$1"
  if [[ ! -f "$SQL_FILE" ]]; then
    echo "Error: SQL file not found: $SQL_FILE" >&2
    exit 1
  fi

  exec docker exec -i "$CONTAINER" \
    psql -X -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DATABASE" < "$SQL_FILE"
fi

# Edit the multiline SQL below for quick, ad hoc queries, then run:
#   ./query_db.sh
#
# Alternatively, leave this query unchanged and run another SQL file:
#   ./query_db.sh sql/inspect_live.sql

docker exec -i "$CONTAINER" \
  psql -X -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DATABASE" <<'SQL'
\pset pager off
\x auto
\timing on

SELECT
    ts,
    symbol,
    wallet,
    side,
    price,
    size
FROM orderflow_events
ORDER BY ts DESC, id DESC
LIMIT 20;
SQL

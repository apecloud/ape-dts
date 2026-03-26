#!/usr/bin/env bash

set -euo pipefail

MODE=""
PG_URL="${PG_URL:-}"
SCHEMAS="${SCHEMAS:-}"

usage() {
  cat <<'EOF'
Usage:
  scripts/pg_replica_identity.sh --mode <check|plan|apply> --url <postgres_url> [--schemas <schema1,schema2,...>]

Examples:
  scripts/pg_replica_identity.sh --mode check --url 'postgres://user:pass@127.0.0.1:5432/postgres'
  scripts/pg_replica_identity.sh --mode plan --url 'postgres://user:pass@127.0.0.1:5432/postgres' --schemas public,test_schema
  scripts/pg_replica_identity.sh --mode apply --url 'postgres://user:pass@127.0.0.1:5432/postgres'

Description:
  check   Print tables that have no primary key and are not configured with replica identity FULL/INDEX.
  plan   Print ALTER TABLE ... REPLICA IDENTITY FULL statements for those tables, but do not execute them.
  apply  Execute ALTER TABLE ... REPLICA IDENTITY FULL for those tables.

Options:
  --mode      Required. One of: check, plan, apply
  --url       Required unless PG_URL is already set.
  --schemas   Optional comma-separated schema allowlist. If omitted, all non-system schemas are checked.
  -h, --help  Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --url)
      PG_URL="${2:-}"
      shift 2
      ;;
    --schemas)
      SCHEMAS="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${MODE}" ]]; then
  echo "--mode is required" >&2
  usage >&2
  exit 1
fi

if [[ -z "${PG_URL}" ]]; then
  echo "--url is required (or set PG_URL)" >&2
  usage >&2
  exit 1
fi

if [[ "${MODE}" != "check" && "${MODE}" != "plan" && "${MODE}" != "apply" ]]; then
  echo "Invalid --mode: ${MODE}" >&2
  usage >&2
  exit 1
fi

SCHEMA_FILTER_SQL=""
if [[ -n "${SCHEMAS}" ]]; then
  SCHEMA_LIST_SQL=$(printf "'%s'" "$(echo "${SCHEMAS}" | sed "s/,/' , '/g")")
  SCHEMA_FILTER_SQL="AND n.nspname IN (${SCHEMA_LIST_SQL})"
fi

read -r -d '' BASE_SQL <<EOF || true
WITH candidate_tables AS (
    SELECT
        c.oid,
        n.nspname AS schema_name,
        c.relname AS table_name,
        c.relkind,
        c.relreplident,
        EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = c.oid
              AND i.indisprimary
        ) AS has_primary_key
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname NOT LIKE 'pg_toast%'
      ${SCHEMA_FILTER_SQL}
)
SELECT
    schema_name,
    table_name,
    CASE relkind
        WHEN 'r' THEN 'table'
        WHEN 'p' THEN 'partitioned table'
        ELSE relkind::text
    END AS table_type,
    CASE relreplident
        WHEN 'd' THEN 'default'
        WHEN 'n' THEN 'nothing'
        WHEN 'f' THEN 'full'
        WHEN 'i' THEN 'index'
        ELSE relreplident::text
    END AS replica_identity,
    format('ALTER TABLE %I.%I REPLICA IDENTITY FULL;', schema_name, table_name) AS alter_sql
FROM candidate_tables
WHERE NOT has_primary_key
  AND relreplident IN ('d', 'n')
ORDER BY schema_name, table_name
EOF

if [[ "${MODE}" == "check" ]]; then
  psql "${PG_URL}" -X -v ON_ERROR_STOP=1 -P pager=off -c "${BASE_SQL}"
elif [[ "${MODE}" == "plan" ]]; then
  psql "${PG_URL}" -X -v ON_ERROR_STOP=1 -P pager=off -t -A -c "SELECT alter_sql FROM (${BASE_SQL}) t"
else
  read -r -d '' APPLY_SQL <<EOF || true
WITH candidate_tables AS (
    SELECT
        c.oid,
        n.nspname AS schema_name,
        c.relname AS table_name,
        c.relkind,
        c.relreplident,
        EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = c.oid
              AND i.indisprimary
        ) AS has_primary_key
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname NOT LIKE 'pg_toast%'
      ${SCHEMA_FILTER_SQL}
)
SELECT format('ALTER TABLE %I.%I REPLICA IDENTITY FULL;', schema_name, table_name)
FROM candidate_tables
WHERE NOT has_primary_key
  AND relreplident IN ('d', 'n')
ORDER BY schema_name, table_name
\gexec
EOF
  psql "${PG_URL}" -X -v ON_ERROR_STOP=1 -P pager=off <<EOF
${APPLY_SQL}
EOF
fi

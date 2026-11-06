#!/usr/bin/env bash

set -Eeuo pipefail

readonly SQLCMD=/opt/mssql-tools18/bin/sqlcmd
readonly INSTALL_SQL=/opt/ape-dts-mssql/install-bigvariant.sql
readonly MAX_ATTEMPTS=60

/opt/mssql/bin/sqlservr &
sqlservr_pid=$!

shutdown_sql_server() {
    if kill -0 "${sqlservr_pid}" 2>/dev/null; then
        kill -TERM "${sqlservr_pid}"
    fi
    wait "${sqlservr_pid}" || true
}

trap shutdown_sql_server TERM INT

export SQLCMDPASSWORD="${MSSQL_SA_PASSWORD:?MSSQL_SA_PASSWORD must be set}"

for ((attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)); do
    if "${SQLCMD}" -S localhost -U sa -C -l 2 -Q "SELECT 1" >/dev/null 2>&1; then
        break
    fi

    if ! kill -0 "${sqlservr_pid}" 2>/dev/null; then
        wait "${sqlservr_pid}"
        exit $?
    fi

    if ((attempt == MAX_ATTEMPTS)); then
        echo "SQL Server did not become ready for BigVariant initialization" >&2
        shutdown_sql_server
        exit 1
    fi

    sleep 2
done

if ! "${SQLCMD}" -S localhost -U sa -C -b -i "${INSTALL_SQL}"; then
    echo "BigVariant initialization failed" >&2
    shutdown_sql_server
    exit 1
fi

unset SQLCMDPASSWORD
wait "${sqlservr_pid}"

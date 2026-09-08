#!/usr/bin/env bash
# Purpose: start MySQL with TLS and row-based binlog settings for snapshot/CDC tests.
# Called on every mysql-tls-{src,dst,cdc-src,cdc-dst} start; the image entrypoint initializes users
# only for an empty data directory. Args: optional extra mysqld options.
set -euo pipefail

exec docker-entrypoint.sh mysqld \
  --server-id=1 \
  --log-bin=mysql-bin \
  --binlog-format=ROW \
  --gtid-mode=ON \
  --enforce-gtid-consistency=ON \
  --ssl-cert=/tls/server/server.crt \
  --ssl-key=/tls/server/server.key \
  --ssl-ca=/tls/client/client-ca.crt \
  "$@"

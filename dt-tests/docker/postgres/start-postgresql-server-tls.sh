#!/usr/bin/env bash
# Purpose: set safe server-key permissions and start PostgreSQL through its image entrypoint.
# Called on every start of postgres-tls-{src,dst}; initialization SQL runs only for
# an empty data directory. Args: optional extra postgres options.
set -euo pipefail

# PostgreSQL rejects a world-readable server private key.
install -d -o postgres -g postgres /var/lib/postgresql/tls
install -m 0600 -o postgres -g postgres /tls/server/server.key /var/lib/postgresql/tls/server.key
exec /usr/local/bin/docker-entrypoint.sh postgres \
  -c ssl=on \
  -c ssl_cert_file=/tls/server/server.crt \
  -c ssl_key_file=/var/lib/postgresql/tls/server.key \
  -c ssl_ca_file=/tls/client/client-ca.crt \
  -c hba_file=/etc/postgresql/pg_hba-tls.conf \
  -c wal_level=logical \
  -c max_replication_slots=16 \
  -c max_wal_senders=16 \
  "$@"

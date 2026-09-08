#!/usr/bin/env bash
# Purpose: prepare the PEM/CA bundle and start MongoDB requiring client certificates.
# Called as TLS standalone destinations' entrypoint on every start; the image entrypoint creates
# users only for an empty data directory. Args: optional extra mongod options.
set -euo pipefail

# MongoDB requires a combined server identity and trusts both test CAs.
cat /tls/server/server.key /tls/server/server.crt > /tmp/mongo-server.pem
cat /tls/server/server-ca.crt /tls/client/client-ca.crt > /tmp/mongo-ca.crt
chown mongodb:mongodb /tmp/mongo-server.pem
chmod 600 /tmp/mongo-server.pem

exec docker-entrypoint.sh mongod \
  --bind_ip_all \
  --tlsMode requireTLS \
  --tlsCertificateKeyFile /tmp/mongo-server.pem \
  --tlsCAFile /tmp/mongo-ca.crt \
  "$@"

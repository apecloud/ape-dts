#!/usr/bin/env bash
# Purpose: prepare the server PEM/CA bundle and start a standalone MongoDB server.
# Called as TLS standalone destinations' entrypoint on every start; the image entrypoint creates
# users only for an empty data directory. Args: optional extra mongod options.
# Env: MONGO_TLS_REQUIRE_CLIENT_CERTIFICATE defaults to false, or true to reject
# clients without certificates.
set -euo pipefail

TLS_CLIENT_OPTIONS=()
case "${MONGO_TLS_REQUIRE_CLIENT_CERTIFICATE:-false}" in
  true) ;;
  false) TLS_CLIENT_OPTIONS+=(--tlsAllowConnectionsWithoutCertificates) ;;
  *) echo "MONGO_TLS_REQUIRE_CLIENT_CERTIFICATE must be true or false" >&2; exit 1 ;;
esac

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
  "${TLS_CLIENT_OPTIONS[@]}" \
  "$@"

#!/bin/sh
# Purpose: start standalone Redis with TLS and the requested client-certificate policy.
# Called on every redis-tls-{source,target}-{7-0,8-0} start before E2E tests.
# Args: <yes|no> for mandatory client certificates; each policy uses its matching CA.
set -eu

AUTH_CLIENTS=${1:?Client-certificate policy (yes or no) is required}
case "$AUTH_CLIENTS" in
  yes) CA_FILE=/tls/client/client-ca.crt ;;
  no) CA_FILE=/tls/server/server-ca.crt ;;
  *) echo "Client-certificate policy must be yes or no" >&2; exit 1 ;;
esac

exec docker-entrypoint.sh redis-server \
  --port 0 \
  --tls-port 6379 \
  --tls-cert-file /tls/server/server.crt \
  --tls-key-file /tls/server/server.key \
  --tls-ca-cert-file "$CA_FILE" \
  --tls-auth-clients "$AUTH_CLIENTS" \
  --requirepass 123456 \
  --save 60 1 \
  --loglevel warning

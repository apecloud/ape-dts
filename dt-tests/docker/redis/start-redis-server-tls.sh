#!/bin/sh
# Purpose: start standalone Redis requiring trusted TLS client certificates.
# Called on every redis-tls-{source,target}-{7-0,8-0} start before E2E tests.
# No arguments; both versions use the shared server and client certificate fixtures.
set -eu

exec docker-entrypoint.sh redis-server \
  --port 0 \
  --tls-port 6379 \
  --tls-cert-file /tls/server/server.crt \
  --tls-key-file /tls/server/server.key \
  --tls-ca-cert-file /tls/client/client-ca.crt \
  --tls-auth-clients yes \
  --requirepass 123456 \
  --save 60 1 \
  --loglevel warning

#!/usr/bin/env bash
# Purpose: start a config server, shard server or mongos requiring TLS client certificates.
# Called on every start of mongo-tls-sharding-{src,dst}-{config,shard,mongos}.
# Args: mongod/mongos and its role-specific options. Replica-set initialization and
# shard registration run separately, after the processes are healthy.
set -euo pipefail

TLS_CERT_SRC=/tls/server/server.crt
TLS_KEY_SRC=/tls/server/server.key
TLS_PEM_DST=/tmp/mongo-server.pem

cat "${TLS_KEY_SRC}" "${TLS_CERT_SRC}" > "${TLS_PEM_DST}"
chown mongodb:mongodb "${TLS_PEM_DST}"
chmod 600 "${TLS_PEM_DST}"
# Trust task client identities and the server identities used between cluster nodes.
cat /tls/server/server-ca.crt /tls/client/client-ca.crt > /tmp/mongo-ca.crt

exec gosu mongodb "$@" \
  --tlsMode requireTLS \
  --tlsCertificateKeyFile "${TLS_PEM_DST}" \
  --tlsCAFile /tmp/mongo-ca.crt \
  --tlsAllowInvalidHostnames

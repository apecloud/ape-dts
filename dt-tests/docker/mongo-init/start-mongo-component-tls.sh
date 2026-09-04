#!/usr/bin/env bash
set -euo pipefail

TLS_CERT_SRC=/tls/server/server.crt
TLS_KEY_SRC=/tls/server/server.key
TLS_PEM_DST=/tmp/mongo-server.pem

cat "${TLS_KEY_SRC}" "${TLS_CERT_SRC}" > "${TLS_PEM_DST}"
chown mongodb:mongodb "${TLS_PEM_DST}"
chmod 600 "${TLS_PEM_DST}"

exec gosu mongodb "$@" \
  --tlsMode requireTLS \
  --tlsCertificateKeyFile "${TLS_PEM_DST}" \
  --tlsCAFile /tls/server/server-ca.crt \
  --tlsAllowConnectionsWithoutCertificates \
  --tlsAllowInvalidHostnames

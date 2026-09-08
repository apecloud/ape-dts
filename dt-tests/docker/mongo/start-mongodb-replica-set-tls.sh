#!/usr/bin/env bash
# Purpose: bootstrap a single-node replica set and root user, then run MongoDB with TLS.
# Called as TLS replica-set sources' entrypoint on every start; bootstrap runs only when the
# initialization marker is absent. Env: MONGO_RS_NAME, MONGO_RS_HOST and
# MONGO_INITDB_ROOT_USERNAME/PASSWORD; MONGO_TLS_REQUIRE_CLIENT_CERTIFICATE defaults
# to false, or true to reject clients without certificates. No positional arguments.
set -euo pipefail

KEYFILE_SRC=/run/secrets/mongo-keyfile
KEYFILE_DST=/tmp/mongo-keyfile
TLS_CERT_SRC=/tls/server/server.crt
TLS_KEY_SRC=/tls/server/server.key
TLS_PEM_DST=/tmp/mongo-server.pem
MARKER=/data/db/.ape_rs_initialized
MONGO_RS_NAME=${MONGO_RS_NAME:-rs0}
MONGO_RS_HOST=${MONGO_RS_HOST:-mongo-tls-src}
MONGO_ROOT_USERNAME=${MONGO_INITDB_ROOT_USERNAME:-root}
MONGO_ROOT_PASSWORD=${MONGO_INITDB_ROOT_PASSWORD:-123456}
TLS_CLIENT_OPTIONS=()
case "${MONGO_TLS_REQUIRE_CLIENT_CERTIFICATE:-false}" in
  true) ;;
  false) TLS_CLIENT_OPTIONS+=(--tlsAllowConnectionsWithoutCertificates) ;;
  *) echo "MONGO_TLS_REQUIRE_CLIENT_CERTIFICATE must be true or false" >&2; exit 1 ;;
esac
if command -v mongosh >/dev/null 2>&1; then
  MONGO_SHELL=mongosh
else
  MONGO_SHELL=mongo
fi

mongo_admin() {
  "${MONGO_SHELL}" "mongodb://127.0.0.1:27017/admin" --quiet "$@"
}

mongo_admin_auth() {
  "${MONGO_SHELL}" \
    "mongodb://${MONGO_ROOT_USERNAME}:${MONGO_ROOT_PASSWORD}@127.0.0.1:27017/admin" \
    --quiet "$@"
}

cp "${KEYFILE_SRC}" "${KEYFILE_DST}"
chown mongodb:mongodb "${KEYFILE_DST}"
chmod 600 "${KEYFILE_DST}"

# MongoDB expects the server certificate and its private key in one PEM file.
cat "${TLS_KEY_SRC}" "${TLS_CERT_SRC}" > "${TLS_PEM_DST}"
chown mongodb:mongodb "${TLS_PEM_DST}"
chmod 600 "${TLS_PEM_DST}"
cat /tls/server/server-ca.crt /tls/client/client-ca.crt > /tmp/mongo-ca.crt

if [ ! -f "${MARKER}" ]; then
  gosu mongodb mongod \
    --bind_ip_all \
    --port 27017 \
    --dbpath /data/db \
    --replSet "${MONGO_RS_NAME}" \
    --keyFile "${KEYFILE_DST}" \
    --fork \
    --logpath /tmp/mongo-bootstrap.log

  until mongo_admin --eval "db.adminCommand('ping').ok" | grep -q 1; do
    sleep 1
  done

  mongo_admin --eval "
    rs.initiate({
      _id: '${MONGO_RS_NAME}',
      members: [{ _id: 0, host: '${MONGO_RS_HOST}:27017' }]
    });
  "

  until mongo_admin --eval "quit(db.adminCommand({ hello: 1 }).isWritablePrimary ? 0 : 1)"; do
    sleep 1
  done

  mongo_admin --eval "
    db.createUser({
      user: '${MONGO_ROOT_USERNAME}',
      pwd: '${MONGO_ROOT_PASSWORD}',
      roles: [{ role: 'root', db: 'admin' }]
    });
  "

  mongo_admin_auth --eval "db.shutdownServer({ force: true })" || true

  until ! mongo_admin_auth --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; do
    sleep 1
  done

  touch "${MARKER}"
fi

exec gosu mongodb mongod \
  --bind_ip_all \
  --port 27017 \
  --dbpath /data/db \
  --replSet "${MONGO_RS_NAME}" \
  --keyFile "${KEYFILE_DST}" \
  --tlsMode requireTLS \
  --tlsCertificateKeyFile "${TLS_PEM_DST}" \
  --tlsCAFile /tmp/mongo-ca.crt \
  "${TLS_CLIENT_OPTIONS[@]}"

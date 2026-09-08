#!/usr/bin/env bash
set -euo pipefail

KEYFILE_SRC=/run/secrets/mongo-keyfile
KEYFILE_DST=/tmp/mongo-keyfile
MARKER=/data/db/.ape_rs_initialized
MONGO_RS_NAME=${MONGO_RS_NAME:-rs0}
MONGO_RS_HOST=${MONGO_RS_HOST:-mongo-src}
MONGO_ROOT_USERNAME=${MONGO_INITDB_ROOT_USERNAME:-root}
MONGO_ROOT_PASSWORD=${MONGO_INITDB_ROOT_PASSWORD:-123456}
if command -v mongosh >/dev/null 2>&1; then
  MONGO_SHELL=mongosh
else
  MONGO_SHELL=mongo
fi

mongo_admin() {
  "${MONGO_SHELL}" "mongodb://127.0.0.1:27017/admin" --quiet "$@"
}

cp "${KEYFILE_SRC}" "${KEYFILE_DST}"
chown mongodb:mongodb "${KEYFILE_DST}"
chmod 600 "${KEYFILE_DST}"

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

  mongo_admin --eval "db.shutdownServer({ force: true })" || true

  until ! mongo_admin --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; do
    sleep 1
  done

  touch "${MARKER}"
fi

exec gosu mongodb mongod \
  --bind_ip_all \
  --port 27017 \
  --dbpath /data/db \
  --replSet "${MONGO_RS_NAME}" \
  --keyFile "${KEYFILE_DST}"

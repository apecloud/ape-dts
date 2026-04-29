#!/usr/bin/env bash
set -euo pipefail

KEYFILE_SRC=/run/secrets/mongo-keyfile
KEYFILE_DST=/tmp/mongo-keyfile
MARKER=/data/db/.ape_rs_initialized
MONGO_RS_NAME=${MONGO_RS_NAME:-rs0}
MONGO_RS_HOST=${MONGO_RS_HOST:-mongo-src}
MONGO_ROOT_USERNAME=${MONGO_INITDB_ROOT_USERNAME:-root}
MONGO_ROOT_PASSWORD=${MONGO_INITDB_ROOT_PASSWORD:-123456}

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

  until mongo admin --host 127.0.0.1 --port 27017 --quiet --eval "db.adminCommand('ping').ok" | grep -q 1; do
    sleep 1
  done

  mongo admin --host 127.0.0.1 --port 27017 --quiet --eval "
    rs.initiate({
      _id: '${MONGO_RS_NAME}',
      members: [{ _id: 0, host: '${MONGO_RS_HOST}:27017' }]
    });
  "

  until mongo admin --host 127.0.0.1 --port 27017 --quiet --eval "quit(db.isMaster().ismaster ? 0 : 1)"; do
    sleep 1
  done

  mongo admin --host 127.0.0.1 --port 27017 --quiet --eval "
    db.createUser({
      user: '${MONGO_ROOT_USERNAME}',
      pwd: '${MONGO_ROOT_PASSWORD}',
      roles: [{ role: 'root', db: 'admin' }]
    });
  "

  mongo admin --host 127.0.0.1 --port 27017 --quiet --eval "db.shutdownServer({ force: true })" || true

  until ! mongo admin --host 127.0.0.1 --port 27017 --quiet --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; do
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

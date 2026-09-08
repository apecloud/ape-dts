#!/usr/bin/env bash
# Purpose: initialize the config-server and shard replica sets and wait for primaries.
# One-shot entrypoint for mongo-tls-sharding-{src,dst}-init, after both servers are
# healthy and before mongos starts. Args: <config-set-name> <config-host:port>
# <shard-set-name> <shard-host:port>.
set -euo pipefail

CONFIG_RS=${1:?Config replica set name is required}
CONFIG_HOST=${2:?Config server address is required}
SHARD_RS=${3:?Shard replica set name is required}
SHARD_HOST=${4:?Shard server address is required}

mongosh "mongodb://${CONFIG_HOST}/admin?tls=true&tlsInsecure=true" --quiet \
  --eval "rs.initiate({ _id: '${CONFIG_RS}', configsvr: true, members: [{ _id: 0, host: '${CONFIG_HOST}' }] })" >/dev/null || true
mongosh "mongodb://${SHARD_HOST}/admin?tls=true&tlsInsecure=true" --quiet \
  --eval "rs.initiate({ _id: '${SHARD_RS}', members: [{ _id: 0, host: '${SHARD_HOST}' }] })" >/dev/null || true

# mongos must wait for both replica sets to elect their primaries.
for target in "$CONFIG_HOST" "$SHARD_HOST"; do
  ready=false
  for _ in $(seq 1 60); do
    if mongosh "mongodb://${target}/admin?tls=true&tlsInsecure=true" --quiet \
      --eval "quit(db.adminCommand({ hello: 1 }).isWritablePrimary ? 0 : 1)" >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [ "$ready" != true ]; then
    echo "MongoDB TLS replica set at ${target} failed to elect a primary" >&2
    exit 1
  fi
done

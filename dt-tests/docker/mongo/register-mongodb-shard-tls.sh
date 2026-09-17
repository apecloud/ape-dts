#!/usr/bin/env bash
# Purpose: register an initialized shard replica set with the MongoDB router.
# One-shot entrypoint for mongo-tls-sharding-{src,dst}-add-shard-init after mongos
# is healthy, before E2E tests. Args: <mongos-host:port> <shard-set-name> <shard-host:port>.
set -euo pipefail

MONGOS_HOST=${1:?Mongos address is required}
SHARD_RS=${2:?Shard replica set name is required}
SHARD_HOST=${3:?Shard server address is required}

exec mongosh "mongodb://${MONGOS_HOST}/admin?tls=true&tlsInsecure=true" \
  --tlsCertificateKeyFile /tls/client/client.pem --quiet \
  --eval "const result = sh.addShard('${SHARD_RS}/${SHARD_HOST}'); if (!result.ok) { printjson(result); quit(1); }"

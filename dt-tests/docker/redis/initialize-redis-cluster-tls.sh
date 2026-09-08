#!/bin/sh
# Purpose: assign all hash slots and join three running TLS nodes into a Redis Cluster.
# One-shot entrypoint for redis-tls-*-init; waits for nodes before configuring the
# cluster and must finish before E2E tests. Args: <node1-tls-port> <node2-tls-port>
# <node3-tls-port> <node2-bus-port> <node3-bus-port>. Env: DT_IT_HOST_IP.
set -eu

NODE1_PORT=${1:?First node TLS port is required}
NODE2_PORT=${2:?Second node TLS port is required}
NODE3_PORT=${3:?Third node TLS port is required}
NODE2_BUS_PORT=${4:?Second node bus port is required}
NODE3_BUS_PORT=${5:?Third node bus port is required}
export REDISCLI_AUTH=123456

redis_cli() {
  port="$1"
  shift
  redis-cli --tls --insecure -h "${DT_IT_HOST_IP:?DT_IT_HOST_IP is required}" -p "$port" "$@"
}

until redis_cli "$NODE1_PORT" ping \
  && redis_cli "$NODE2_PORT" ping \
  && redis_cli "$NODE3_PORT" ping; do
  sleep 1
done

cluster_ok() {
  redis_cli "$NODE1_PORT" cluster info 2>/dev/null | grep -q 'cluster_state:ok' \
    && redis_cli "$NODE1_PORT" cluster info 2>/dev/null | grep -q 'cluster_slots_assigned:16384'
}

# Assign all hash slots before introducing the three primaries to each other.
if ! cluster_ok; then
  redis_cli "$NODE1_PORT" cluster addslots $(seq 0 5460)
  redis_cli "$NODE2_PORT" cluster addslots $(seq 5461 10922)
  redis_cli "$NODE3_PORT" cluster addslots $(seq 10923 16383)
  redis_cli "$NODE1_PORT" cluster set-config-epoch 1
  redis_cli "$NODE2_PORT" cluster set-config-epoch 2
  redis_cli "$NODE3_PORT" cluster set-config-epoch 3
  redis_cli "$NODE1_PORT" cluster meet "$DT_IT_HOST_IP" "$NODE2_PORT" "$NODE2_BUS_PORT"
  redis_cli "$NODE1_PORT" cluster meet "$DT_IT_HOST_IP" "$NODE3_PORT" "$NODE3_BUS_PORT"
fi

for _ in $(seq 1 10); do
  if cluster_ok; then
    exit 0
  fi
  sleep 2
done

echo "Redis TLS cluster at ${DT_IT_HOST_IP}:${NODE1_PORT} failed to become ready" >&2
redis_cli "$NODE1_PORT" cluster nodes || true
redis_cli "$NODE1_PORT" cluster info || true
exit 1

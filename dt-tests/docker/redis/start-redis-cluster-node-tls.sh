#!/bin/sh
# Purpose: start a Redis Cluster node requiring TLS client certificates, with host-reachable addresses.
# Called on every start of a redis-tls-*-node* service, before cluster initialization.
# Args: <host-tls-port> <host-cluster-bus-port>. Env: DT_IT_HOST_IP.
set -eu

# Trust task client identities and the server identities used on the cluster bus.
cat /tls/server/server-ca.crt /tls/client/client-ca.crt > /tmp/redis-ca.crt

# Announce host-mapped ports so both the test client and peer nodes can connect.
exec redis-server \
  --port 0 \
  --tls-port 6379 \
  --tls-cert-file /tls/server/server.crt \
  --tls-key-file /tls/server/server.key \
  --tls-ca-cert-file /tmp/redis-ca.crt \
  --tls-auth-clients yes \
  --tls-cluster yes \
  --requirepass 123456 \
  --masterauth 123456 \
  --cluster-enabled yes \
  --cluster-config-file nodes.conf \
  --cluster-node-timeout 5000 \
  --appendonly yes \
  --cluster-announce-ip "${DT_IT_HOST_IP:?DT_IT_HOST_IP is required}" \
  --cluster-announce-port 0 \
  --cluster-announce-tls-port "${1:?TLS announce port is required}" \
  --cluster-announce-bus-port "${2:?Cluster bus announce port is required}"

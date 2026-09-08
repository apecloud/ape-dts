#!/usr/bin/env bash
# Purpose: regenerate the independent test client CA and client identity files.
# Run manually before mutual-TLS tests when replacing client fixtures, not during
# container startup. No arguments; overwrites this directory's client CA/PEM files.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# This independent CA signs only test client identities, not server certificates.
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 3650 \
  -keyout client-ca.key -out client-ca.crt -config openssl.cnf -extensions v3_ca
openssl req -newkey rsa:2048 -nodes -sha256 \
  -keyout client.key -out client.csr -subj "/CN=ape_dts"
openssl x509 -req -sha256 -days 3650 \
  -in client.csr -CA client-ca.crt -CAkey client-ca.key -CAcreateserial \
  -out client.crt -extfile openssl.cnf -extensions client_cert

# MongoDB takes a combined PEM identity; other engines can use the separate files.
cat client.crt client.key > client.pem

rm -f client-ca.srl client.csr
chmod 0600 client-ca.key
# Public test fixtures must be readable by unprivileged database containers.
chmod 0644 client-ca.crt client.crt client.key client.pem

#!/usr/bin/env bash
# Purpose: regenerate the shared test server CA, certificate and private keys.
# Run manually before tests when replacing server fixtures, not during container
# startup. No arguments; overwrites this directory's server CA and identity files.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Regenerate the test-only CA and the shared database server certificate.
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 3650 \
  -keyout server-ca.key -out server-ca.crt -config openssl.cnf -extensions v3_ca
openssl req -newkey rsa:2048 -nodes -sha256 \
  -keyout server.key -out server.csr -subj "/CN=localhost"
openssl x509 -req -sha256 -days 3650 \
  -in server.csr -CA server-ca.crt -CAkey server-ca.key -CAcreateserial \
  -out server.crt -extfile openssl.cnf -extensions server_cert

rm -f server-ca.srl server.csr
chmod 0644 server-ca.crt server.crt
chmod 0600 server-ca.key
chmod 0644 server.key

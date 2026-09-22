# Database TLS server fixtures

These OpenSSL-generated certificates are fixtures for local and CI integration tests only. They
are not secrets and must not be used outside the test environment.

Run `./generate.sh` in this directory to regenerate the server CA and server certificate. Database
clients use the public `server-ca.crt` as their trust anchor.

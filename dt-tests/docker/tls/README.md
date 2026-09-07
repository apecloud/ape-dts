# Database TLS fixtures

`server/` contains the OpenSSL configuration, generation script, CA, and shared database server
certificate. Run its `generate.sh` from any working directory to regenerate the fixtures.

The fixtures are for local and CI integration tests only. They are not secrets and must not be used
outside the test environment.

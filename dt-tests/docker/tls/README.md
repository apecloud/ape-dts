# Shared Database TLS Fixtures

These keys and certificates are public test fixtures, not production credentials.

- `server/generate.sh` generates the server CA and certificate (SAN: `localhost`).
- `client/generate.sh` generates an independent client CA and identity, including
  `client.pem` for MongoDB's combined certificate/private-key input, and a separate
  self-signed `untrusted-client.pem` for server rejection tests.
- The scripts and their OpenSSL configuration files live alongside their outputs.
- Clients trust `server/server-ca.crt`. Servers requiring mutual TLS trust
  `client/client-ca.crt`. MongoDB's test trust bundle contains both CAs.
- Regenerate each side independently with Bash and OpenSSL. No client-generation
  script reads or copies the server CA.

Engine-specific startup and initialization files live in `../mysql/`,
`../postgres/`, `../mssql/`, `../mongo/`, and `../redis/`, with a `-tls` suffix
before the file extension. This directory only holds shared certificate fixtures
and their generation scripts/configuration.

Startup scripts use readable `start-<engine>-<role>-tls.sh` names. One-shot topology
setup uses `initialize-...` or `register-...`, distinct from starting a server process.
Each script starts with its purpose, invocation timing, arguments and relevant
environment variables. SQL initialization files also document their first-run timing.

MySQL, PostgreSQL, MongoDB and Redis scripts pass fixed TLS/startup settings as
command-line options. Compose selects the entrypoint and only passes differences
such as cluster ports, MongoDB roles and the Redis client-certificate policy. PostgreSQL
still needs `../postgres/pg_hba-tls.conf` for client-certificate authentication
rules, selected with `-c hba_file=...`; its startup script also prepares private-key
permissions. SQL Server 2022 uses `../mssql/mssql-tls.conf`, mounted read-only,
because its TLS certificate settings are configured through
[`mssql.conf`](https://learn.microsoft.com/en-us/sql/linux/security/encrypted-connections?view=sql-server-ver16#configure-sql-server).
No startup script edits the database configuration files. Non-TLS PostgreSQL
services mount only `../postgres/initdb.d/`, preserving the existing init SQL
without executing TLS scripts or exposing additional init scripts from the image.
Redis cluster and MongoDB sharding initialization scripts are also kept in their
engine's directory; Compose passes each topology's addresses and ports.

All TLS tests and fixtures live under `dt-tests/tests/tls/`. Engine, topology and
version variants are sibling directories; fixtures inside each directory are grouped
by task, not SSL mode:

```text
tls/
  mysql/                struct/ snapshot/ cdc/ checker/
  pg/                   struct/ snapshot/ cdc/ checker/
  mssql/                snapshot/
  mongo/                struct/ snapshot/ cdc/
  mongo_shard/          struct/ snapshot/ cdc/
  redis_7_0/            snapshot_and_cdc/
  redis_8_0/            snapshot_and_cdc/
  redis_cluster_6_2/    snapshot_and_cdc/
  redis_cluster_7_0/    snapshot_and_cdc/
```

Each task has one set of prepare/test data and a base `task_config.ini`. The
`tls_task_tests!` test matrix supplies `ssl_mode` and `ssl_allow_invalid_hostnames` to
the shared harness, which applies overrides to the generated config before opening
source/destination clients or running the task. Each parameter combination remains
a separate serial test, so SSL modes share fixtures but report failures independently.
CA and client identity paths continue to come from environment placeholders.

`tls_task_tests!` invokes `run_tls_task_test` to prepare data, execute a DTS task
and check its results. Separate `tls_connection_validation` tests exercise connection
acceptance/rejection for CA, hostname and client-identity settings without running
full data-transfer tasks.

MongoDB's `verify_full` snapshot reuses `mongo/snapshot` data with URLs from
`mongo_mtls_extractor_url` and `mongo_mtls_sinker_url`. These point to two dedicated
containers, `mongo-mtls-src` (replica set) and `mongo-mtls-dst` (standalone), whose
startup scripts omit `--tlsAllowConnectionsWithoutCertificates` when
`MONGO_TLS_REQUIRE_CLIENT_CERTIFICATE=true`. Their health checks present a trusted
client certificate and verify the server's certificate and hostname.

`server_requires_trusted_client_certificate` checks both endpoints' effective TLS
options, successful connections with the trusted client identity, and rejection of
missing or self-signed untrusted client identities. It reconnects with the trusted
identity after each rejection to distinguish certificate failures from server outages.
The existing `tls_connection_validation` covers the client's server CA/hostname
validation; `verify_full` itself remains a client verification mode, while mandatory
client certificates are a separate server policy. Database login still uses the
configured username/password, not `MONGODB-X509` authentication.

The runner keeps
three independent suites, each starting only its required services. With
`--down-each-suite`, the runner cleans up the integration Compose project between suites:

| Suite | Test namespace | Engines |
| --- | --- | --- |
| `tls` | `tls::{mysql,pg,mssql}` | MySQL, PostgreSQL, MSSQL |
| `mongo_to_mongo_tls` | `tls::{mongo,mongo_shard}` | MongoDB replica set, standalone and sharding |
| `redis_to_redis_tls` | `tls::redis_{7_0,8_0}`, `tls::redis_cluster_{6_2,7_0}` | Redis 6.2, 7.0, 8.0, including clusters |

The MongoDB suite starts 10 database containers plus 4 one-shot initialization
containers (14 total). Its `require` and sharding tests keep their existing endpoints.

Run them separately, or sequentially with cleanup between suites from `dt-tests`:

```bash
./scripts/run-integration-tests.sh \
  --suite tls --suite mongo_to_mongo_tls --suite redis_to_redis_tls \
  --all --down-each-suite
```

The relational suite covers all four SSL modes for
MySQL/PostgreSQL struct, snapshot and checker, PostgreSQL CDC, and MSSQL snapshot (the only
MSSQL extraction workflow supported on this branch). Tests check actual session
encryption, hostname/CA rejection, and client-certificate authentication. MySQL
CDC covers only `disable`/`require`: the current binlog driver supports neither
certificate verification nor client identity files, and unsupported modes are rejected.

`ssl_allow_invalid_hostnames=true` is exercised separately. The current SQLx
rustls verifier does not handle rustls's `NotValidForNameContext` error, so its
hostname opt-out remains ineffective; MongoDB/rustls and Tiberius expose no
independent opt-out. PG CDC (OpenSSL) and Redis support it without disabling
CA verification.

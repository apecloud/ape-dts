# Redis -> Redis templates

Refer to [config details](/docs/en/config.md) for explanations of common fields.

ape-dts uses PSYNC to synchronize Redis data.

- Snapshot: only migrates the snapshot, which is the RDB returned by PSYNC.
- Snapshot + CDC: migrates the snapshot and synchronizes incremental data, including the RDB and AOF.
- CDC: receive but discard RDB (if PSYNC returns RDB), only synchronizes the AOF.

## TLS

Redis source and target URLs support `redis://` and `rediss://`. An explicit `ssl_mode` overrides
the URL scheme:

- `ssl_mode=disable`: plaintext.
- `ssl_mode=require`: TLS encryption without server certificate verification.
- `ssl_mode=verify_ca`: TLS with CA chain and hostname/IP verification against the certificate SAN.
- `ssl_mode=verify_full`: TLS with CA chain and hostname/IP verification against the certificate SAN.

`ssl_allow_invalid_hostnames` defaults to `false`. Set it to `true` to skip only hostname/IP
verification, retaining CA verification. For mutual TLS, set `ssl_client_cert_path` and `ssl_client_key_path`.

`ssl_ca_path` is required for `verify_ca` and `verify_full`. Without an explicit `ssl_mode`, `rediss://` selects
`require`. An explicit mode overrides the URL scheme and `#insecure` fragment. The same settings
protect the source PSYNC replication stream and ordinary command connections. In Cluster mode,
both verification modes require each node's certificate to be signed by the configured CA;
`verify_full` also requires each discovered node hostname or IP to match its certificate SAN.

Example:

```ini
[extractor]
db_type=redis
extract_type=snapshot_and_cdc
url=redis://:123456@redis-source.example.com:6380
ssl_mode=verify_ca
ssl_ca_path=/etc/ssl/certs/redis-ca.pem

[sinker]
db_type=redis
sink_type=write
url=rediss://:123456@redis-target.example.com:6390
ssl_mode=verify_ca
ssl_ca_path=/etc/ssl/certs/redis-ca.pem
```

# Snapshot
```
[extractor]
db_type=redis
extract_type=snapshot
repl_port=10008
url=redis://:123456@127.0.0.1:6380

[filter]
do_dbs=*
do_events=
ignore_dbs=1,2
ignore_tbs=
do_tbs=

[sinker]
db_type=redis
sink_type=write
url=redis://:123456@127.0.0.1:6390
batch_size=200

[router]
db_map=
col_map=
tb_map=

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[parallelizer]
parallel_type=redis
parallel_size=8

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

- [extractor]

| Config | Description | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| repl_port | ape_dts uses PSYNC to pull Redis data, repl_port is used in "replconf listening-port [port]" command | 10008 | 10008 |

# Snapshot + CDC
```
[extractor]
db_type=redis
extract_type=snapshot_and_cdc
repl_port=10008
url=redis://:123456@127.0.0.1:6380

[filter]
do_dbs=*
do_events=
ignore_dbs=1,2
ignore_tbs=
do_tbs=
ignore_cmds=flushall,flushdb

[sinker]
db_type=redis
sink_type=write
method=restore
url=redis://:123456@127.0.0.1:6390
batch_size=200

[router]
db_map=
col_map=
tb_map=

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[parallelizer]
parallel_type=redis
parallel_size=8

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# CDC
```
[extractor]
db_type=redis
extract_type=cdc
repl_port=10008
url=redis://:123456@127.0.0.1:6380

[filter]
do_dbs=*
do_events=
ignore_dbs=1,2
ignore_tbs=
do_tbs=
ignore_cmds=flushall,flushdb

[sinker]
db_type=redis
sink_type=write
method=restore
url=redis://:123456@127.0.0.1:6390
batch_size=200

[router]
db_map=
col_map=
tb_map=

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[parallelizer]
parallel_type=redis
parallel_size=8

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

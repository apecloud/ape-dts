# Start A Mysql to Mysql test

```bash
cd dt-tests/docker
docker compose -f docker-compose.yml up --detach --wait mysql-src mysql-dst mysql-meta
cd ../tests
export CMAKE_POLICY_VERSION_MINIMUM=3.5 && cargo nextest run --package dt-tests --test integration_test --no-fail-fast --test-threads 1 mysql_to_mysql
cd ../docker
docker compose -f docker-compose.yml down --volumes --remove-orphans
```

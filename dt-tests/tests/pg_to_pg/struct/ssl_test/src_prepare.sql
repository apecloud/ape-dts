DROP SCHEMA IF EXISTS struct_ssl_it_pg2pg CASCADE;

CREATE SCHEMA struct_ssl_it_pg2pg;

```
CREATE TABLE struct_ssl_it_pg2pg.ssl_test_table (
    name VARCHAR(100),
    value INT DEFAULT NULL
);
```

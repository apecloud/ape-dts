-- CREATE DATABASE euc_cn_db WITH ENCODING = 'EUC_CN' LC_COLLATE = 'C' LC_CTYPE = 'C' TEMPLATE=template0;

DROP TABLE IF EXISTS bytea_pk_test;

```
CREATE TABLE bytea_pk_test (
    category_id VARCHAR(50),      -- composite primary key part 1 (text)
    binary_id   BYTEA,            -- composite primary key part 2 (binary)
    description TEXT,             -- description to verify content correspondence
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- composite primary key
    CONSTRAINT pk_bytea_pk_test PRIMARY KEY (category_id, binary_id)
);
```
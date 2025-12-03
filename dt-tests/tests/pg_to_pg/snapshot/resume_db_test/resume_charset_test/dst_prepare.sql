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

insert into apecloud_resumer_test.ape_task_position (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'public-bytea_pk_test', '{"type":"RdbSnapshot","db_type":"pg","schema":"public","tb":"bytea_pk_test","order_col_values":{"binary_id":"c4e3bac3cac0bde7","category_id":"cat1"}}');
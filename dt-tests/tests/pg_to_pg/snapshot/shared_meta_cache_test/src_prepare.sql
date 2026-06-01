DROP SCHEMA IF EXISTS meta_cache_test_db CASCADE;
CREATE SCHEMA meta_cache_test_db;

CREATE TABLE meta_cache_test_db.same_schema_fetch (
  id int NOT NULL,
  payload varchar(64) NOT NULL,
  amount numeric(10, 2) DEFAULT NULL,
  updated_at timestamp NULL DEFAULT NULL,
  PRIMARY KEY (id)
);

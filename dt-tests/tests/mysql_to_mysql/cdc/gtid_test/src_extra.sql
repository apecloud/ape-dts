CREATE TABLE test_db_1.tx_source (id INT PRIMARY KEY, value INT) ENGINE=InnoDB;
INSERT INTO test_db_1.tx_source VALUES (1, 10), (2, 20), (3, 30), (4, 40);

-- An empty GTID transaction ends with Query COMMIT; the next GTID must reset it.
```
SET @empty_gtid = CONCAT(UUID(), ':1');
SET @@SESSION.GTID_NEXT = @empty_gtid;
BEGIN;
COMMIT;
SET @@SESSION.GTID_NEXT = 'AUTOMATIC';
```

CREATE TABLE test_db_1.after_empty (id INT PRIMARY KEY, value INT) ENGINE=InnoDB;
INSERT INTO test_db_1.after_empty VALUES (1, 100);

-- Consecutive empty transactions must not retain an older transaction's start.
```
SET @empty_gtid = CONCAT(UUID(), ':1');
SET @@SESSION.GTID_NEXT = @empty_gtid;
BEGIN;
COMMIT;
SET @@SESSION.GTID_NEXT = 'AUTOMATIC';
```
```
SET @empty_gtid = CONCAT(UUID(), ':1');
SET @@SESSION.GTID_NEXT = @empty_gtid;
BEGIN;
COMMIT;
SET @@SESSION.GTID_NEXT = 'AUTOMATIC';
```

UPDATE test_db_1.tx_source SET value = 11 WHERE id = 1;

-- The next transaction can contain both a CREATE TABLE and copied rows.
```
SET @empty_gtid = CONCAT(UUID(), ':1');
SET @@SESSION.GTID_NEXT = @empty_gtid;
BEGIN;
COMMIT;
SET @@SESSION.GTID_NEXT = 'AUTOMATIC';
```

CREATE TABLE test_db_1.ctas_after_empty ENGINE=InnoDB AS SELECT * FROM test_db_1.tx_source;

-- A compressed transaction must keep its outer physical replay position.
```
SET @empty_gtid = CONCAT(UUID(), ':1');
SET @@SESSION.GTID_NEXT = @empty_gtid;
BEGIN;
COMMIT;
SET @@SESSION.GTID_NEXT = 'AUTOMATIC';
```
```
SET SESSION binlog_transaction_compression = ON;
INSERT INTO test_db_1.tx_source VALUES (5, 50), (6, 60);
SET SESSION binlog_transaction_compression = OFF;
```

INSERT INTO test_db_1.one_pk_no_uk_1 VALUES(1, 1),(2, 2),(3, 3),(4, 4),(5, 5),(6, 6),(7, 7),(8, 8);
INSERT INTO test_db_2.one_pk_no_uk_1 VALUES(1, 1),(2, 2),(3, 3),(4, 4),(5, 5),(6, 6),(7, 7),(8, 8);
ANALYZE test_db_1.one_pk_no_uk_1;
ANALYZE test_db_2.one_pk_no_uk_1;

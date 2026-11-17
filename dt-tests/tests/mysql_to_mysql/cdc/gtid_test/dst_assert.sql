SELECT CASE WHEN GROUP_CONCAT(CONCAT(id, ':', value) ORDER BY id) = '1:11,2:20,3:30,4:40,5:50,6:60' THEN 'ok' ELSE 'failed' END FROM test_db_1.tx_source;
SELECT CASE WHEN GROUP_CONCAT(CONCAT(id, ':', value) ORDER BY id) = '1:100' THEN 'ok' ELSE 'failed' END FROM test_db_1.after_empty;
SELECT CASE WHEN GROUP_CONCAT(CONCAT(id, ':', value) ORDER BY id) = '1:11,2:20,3:30,4:40' THEN 'ok' ELSE 'failed' END FROM test_db_1.ctas_after_empty;

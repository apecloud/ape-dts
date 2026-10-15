SELECT CASE WHEN (SELECT COUNT(*) FROM `rules_inventory`.`orders`) = 3 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM `rules_inventory`.`orders` WHERE secret IS NULL AND note = 'keep"quote') = 3 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM `rules_inventory`.`ddl_allowed` WHERE extra = 7) = 1 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM mysql.user WHERE User = 'rules_allowed') = 1 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM mysql.tables_priv WHERE User = 'rules_allowed' AND Db = 'rules_inventory' AND Table_name = 'orders' AND Table_priv = 'Select') = 1 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM `rules_inventory`.`orders` WHERE unused IS NOT NULL) = 0 THEN 'ok' ELSE 'failed' END;

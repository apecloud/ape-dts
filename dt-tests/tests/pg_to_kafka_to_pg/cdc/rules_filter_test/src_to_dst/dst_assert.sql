SELECT CASE WHEN (SELECT COUNT(*) FROM "rules_inventory"."orders") = 3 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM "rules_inventory"."orders" WHERE secret IS NULL AND note = 'keep"quote') = 3 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM "rules_inventory"."ddl_allowed" WHERE extra = 7) = 1 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM "rules_inventory"."orders" WHERE unused IS NOT NULL) = 0 THEN 'ok' ELSE 'failed' END;

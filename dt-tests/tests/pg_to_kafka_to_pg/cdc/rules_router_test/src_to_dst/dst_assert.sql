SELECT CASE WHEN (SELECT COUNT(*) FROM "rules_archive"."ddl_renamed" WHERE extra = 7) = 1 THEN 'ok' ELSE 'failed' END;

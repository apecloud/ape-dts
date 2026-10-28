SELECT CASE WHEN (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'rules_struct' AND table_name = 'blocked') = 0 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'rules_struct' AND table_name = 'star_skip') = 0 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'rules_struct_hidden' AND table_name = 'blocked') = 0 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'rules_struct_hidden' AND table_name = 'rescued') = 0 THEN 'ok' ELSE 'failed' END;
SELECT CASE WHEN (SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'rules_struct_hidden') = 0 THEN 'ok' ELSE 'failed' END;

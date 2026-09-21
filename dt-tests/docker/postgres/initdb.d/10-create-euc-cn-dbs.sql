SELECT 'CREATE DATABASE postgres_euc_cn ENCODING ''EUC_CN'' LC_COLLATE ''C'' LC_CTYPE ''C'' TEMPLATE template0'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'postgres_euc_cn')
\gexec

SELECT 'CREATE DATABASE euc_cn_db ENCODING ''EUC_CN'' LC_COLLATE ''C'' LC_CTYPE ''C'' TEMPLATE template0'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'euc_cn_db')
\gexec

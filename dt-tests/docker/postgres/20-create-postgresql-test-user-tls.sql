-- Purpose: create the PostgreSQL test role matching the client certificate's CN.
-- Run by postgres-tls-{src,dst}'s image entrypoint only when initializing an empty
-- data directory; remote client-certificate requirements come from pg_hba-tls.conf.
CREATE ROLE ape_dts LOGIN SUPERUSER PASSWORD '123456';

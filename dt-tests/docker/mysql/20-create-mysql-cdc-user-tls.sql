-- Purpose: create the encryption-only account in both MySQL CDC TLS containers;
-- the source binlog client cannot send a client certificate yet.
-- Run by mysql-tls-cdc-{src,dst}'s image entrypoint when initializing an empty data directory.
CREATE USER 'ape_cdc'@'%' IDENTIFIED BY '123456' REQUIRE SSL;
GRANT ALL PRIVILEGES ON *.* TO 'ape_cdc'@'%';

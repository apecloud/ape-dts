-- Purpose: create mutual-TLS and encryption-only CDC accounts for MySQL tests.
-- Run by mysql-tls-{src,dst}'s image entrypoint only when initializing an empty
-- data directory, before the database becomes ready for E2E tests.
CREATE USER 'ape_dts'@'%' IDENTIFIED BY '123456' REQUIRE X509;
GRANT ALL PRIVILEGES ON *.* TO 'ape_dts'@'%';
CREATE USER 'ape_cdc'@'%' IDENTIFIED BY '123456' REQUIRE SSL;
GRANT ALL PRIVILEGES ON *.* TO 'ape_cdc'@'%';

INSERT INTO tls_test.accounts VALUES (1, 'first'), (2, 'second');
UPDATE tls_test.accounts SET value='updated' WHERE id=1;
DELETE FROM tls_test.accounts WHERE id=2;
INSERT INTO tls_test.accounts VALUES (3, 'last');

CREATE TABLE rules_inventory.ddl_created (id INT PRIMARY KEY, note VARCHAR(80));
ALTER TABLE rules_inventory.ddl_created ADD COLUMN extra INT;
ALTER TABLE rules_inventory.ddl_created RENAME TO ddl_renamed;
INSERT INTO rules_inventory.ddl_renamed VALUES (1,'ddl routed',7);

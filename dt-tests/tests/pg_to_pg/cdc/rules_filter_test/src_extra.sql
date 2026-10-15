UPDATE rules_inventory.orders SET note = 'ignored update' WHERE id = 1;
DELETE FROM rules_inventory.orders WHERE id = 2;
CREATE TABLE rules_inventory.ddl_allowed (id INT PRIMARY KEY, note VARCHAR(80));
ALTER TABLE rules_inventory.ddl_allowed ADD COLUMN extra INT;
INSERT INTO rules_inventory.ddl_allowed VALUES (1,'retained',7);
TRUNCATE TABLE rules_inventory.ddl_allowed;

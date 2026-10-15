INSERT INTO "rules_special"."renamed" VALUES (1,'first','secret'),(2,'second','secret');
INSERT INTO "rules_archive"."audit" VALUES (1,'first','secret'),(2,'second','secret');
INSERT INTO "rules_final"."unchained" VALUES (1,'first','secret'),(2,'second','secret');
INSERT INTO "rules_plain"."identity" VALUES (1,'first','secret'),(2,'second','secret');
INSERT INTO "rules_special"."renamed,log:2" VALUES (1,'first','secret'),(2,'second','secret');
UPDATE rules_special.renamed SET label = 'changed' WHERE order_id = 1;
DELETE FROM rules_special.renamed WHERE order_id = 2;
INSERT INTO "rules_special"."star_renamed" VALUES (1,'first','secret'),(2,'second','secret');
INSERT INTO "rules_archive"."literal_other" VALUES (1,'first','secret'),(2,'second','secret');

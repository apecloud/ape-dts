DROP SCHEMA IF EXISTS "rules_struct" CASCADE;
CREATE SCHEMA "rules_struct";
CREATE TABLE "rules_struct"."orders" ("id" INT PRIMARY KEY, "note" VARCHAR(80), "secret" VARCHAR(80));
CREATE INDEX idx_note_orders ON rules_struct.orders(note);
CREATE TABLE "rules_struct"."audit" ("id" INT PRIMARY KEY, "note" VARCHAR(80), "secret" VARCHAR(80));
CREATE INDEX idx_note_audit ON rules_struct.audit(note);
CREATE TABLE "rules_struct"."blocked" ("id" INT PRIMARY KEY, "note" VARCHAR(80), "secret" VARCHAR(80));
CREATE INDEX idx_note_blocked ON rules_struct.blocked(note);

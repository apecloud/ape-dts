drop schema if exists struct_it_pg2pg_rbac CASCADE;
DROP ROLE IF EXISTS r1;
DROP ROLE IF EXISTS r2;
DROP ROLE IF EXISTS r3;
DROP ROLE IF EXISTS r4;
DROP ROLE IF EXISTS r5;
DROP ROLE IF EXISTS r_seq;


create schema struct_it_pg2pg_rbac;

CREATE TABLE struct_it_pg2pg_rbac.test_1 (
  field1 VARCHAR(255) NOT NULL,
  field2 VARCHAR(255) NOT NULL,
  field3 VARCHAR(255) NOT NULL
);

CREATE TABLE struct_it_pg2pg_rbac.test_2 (
  field1 VARCHAR(255) NOT NULL,
  field2 VARCHAR(255) NOT NULL,
  field3 VARCHAR(255) NOT NULL
);

CREATE SEQUENCE struct_it_pg2pg_rbac.custom_seq1
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
CREATE TABLE struct_it_pg2pg_rbac.test_3 (
  field1 SERIAL PRIMARY KEY,
  field2 VARCHAR(255) NOT NULL DEFAULT nextval('struct_it_pg2pg_rbac.custom_seq1'::regclass),
  field3 VARCHAR(255) 
);

create role r1 nologin password '123456';
create role r2 login password '123456';
create role r3 login password '123456';
create role r4 login password '123456';
create role r5 login noinherit password '123456';
create role r_seq login password '123456'; 

GRANT USAGE ON SCHEMA struct_it_pg2pg_rbac TO r2;
GRANT USAGE ON SCHEMA struct_it_pg2pg_rbac TO r3;
GRANT USAGE ON SCHEMA struct_it_pg2pg_rbac TO r_seq;

GRANT SELECT ON struct_it_pg2pg_rbac.test_1 TO r2;
GRANT SELECT (field1) ON struct_it_pg2pg_rbac.test_2 TO r2;
GRANT SELECT ON struct_it_pg2pg_rbac.test_2 TO r3;
GRANT ALL ON struct_it_pg2pg_rbac.test_3 TO r_seq;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA struct_it_pg2pg_rbac TO r_seq;
GRANT USAGE ON SEQUENCE struct_it_pg2pg_rbac.custom_seq1 TO r_seq;

-- role member
GRANT r2 to r4;
GRANT r2 to r5;


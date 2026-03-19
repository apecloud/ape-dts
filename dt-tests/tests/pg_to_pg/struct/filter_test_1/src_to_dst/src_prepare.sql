drop schema if exists struct_it_pg2pg_1 CASCADE;

create schema struct_it_pg2pg_1;

-- all index types:
CREATE TABLE struct_it_pg2pg_1.full_index_type (id SERIAL PRIMARY KEY,unique_col VARCHAR(255) NOT NULL,index_col VARCHAR(255),fulltext_col TSVECTOR,spatial_col POINT NOT NULL,simple_index_col VARCHAR(255),composite_index_col1 VARCHAR(255),composite_index_col2 VARCHAR(255),composite_index_col3 VARCHAR(255),check_col VARCHAR(255));

-- indexes
CREATE UNIQUE INDEX unique_index ON struct_it_pg2pg_1.full_index_type (unique_col);

CREATE INDEX index_index ON struct_it_pg2pg_1.full_index_type (index_col);

CREATE INDEX fulltext_index ON struct_it_pg2pg_1.full_index_type USING gin(fulltext_col);

CREATE INDEX spatial_index ON struct_it_pg2pg_1.full_index_type USING gist(spatial_col);

CREATE INDEX simple_index ON struct_it_pg2pg_1.full_index_type (simple_index_col);

CREATE INDEX composite_index ON struct_it_pg2pg_1.full_index_type (composite_index_col1, composite_index_col2, composite_index_col3);

-- table comments:
COMMENT ON TABLE struct_it_pg2pg_1.full_index_type IS 'Comment on full_index_type.';

-- column comments:
COMMENT ON COLUMN struct_it_pg2pg_1.full_index_type.id IS 'Comment on full_index_type.id.';

-- constraints
ALTER TABLE "struct_it_pg2pg_1"."full_index_type" ADD CONSTRAINT "full_index_type_check_col_check" CHECK ((char_length((check_col)::text) > 3));

-- foreign constraints
CREATE TABLE struct_it_pg2pg_1.foreign_key_parent (pk SERIAL, parent_col_1 INTEGER UNIQUE, parent_col_2 INTEGER UNIQUE, PRIMARY KEY(pk));
CREATE TABLE struct_it_pg2pg_1.foreign_key_child (pk SERIAL, child_col_1 INTEGER UNIQUE, child_col_2 INTEGER UNIQUE, PRIMARY KEY(pk));
ALTER TABLE struct_it_pg2pg_1.foreign_key_child ADD CONSTRAINT fk_test_1 FOREIGN KEY (child_col_1) REFERENCES struct_it_pg2pg_1.foreign_key_parent (parent_col_1);
ALTER TABLE struct_it_pg2pg_1.foreign_key_child ADD CONSTRAINT fk_test_2 FOREIGN KEY (child_col_2) REFERENCES struct_it_pg2pg_1.foreign_key_parent (parent_col_2);
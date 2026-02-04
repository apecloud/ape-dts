CREATE EXTENSION IF NOT EXISTS plpgsql;
CREATE EXTENSION IF NOT EXISTS ltree;
CREATE EXTENSION IF NOT EXISTS isn;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS default_table;
CREATE TABLE default_table(pk serial, val numeric(20,8), created_at timestamp, created_at_tz timestamptz, ctime time , ctime_tz timetz , cdate date , cmoney money , cbits bit(3) , csmallint smallint , cinteger integer , cbigint bigint , creal real , cbool bool , cfloat8 float8 , cnumeric numeric(6,2) , cvarchar varchar(5) , cbox box , ccircle circle , cinterval interval , cline line , clseg lseg , cpath path , cpoint point , cpolygon polygon , cchar char , ctext text , cjson json , cxml xml , cuuid uuid , cvarbit varbit(3) , cinet inet , ccidr cidr , cmacaddr macaddr , PRIMARY KEY(pk));

DROP TABLE IF EXISTS col_has_special_character_table;
CREATE TABLE col_has_special_character_table ("p,k,1" text, "p\k&2" text, "col`1" text, "col:2" text, "col\3" text, PRIMARY KEY("p,k,1", "p\k&2"));

DROP TABLE IF EXISTS ignore_cols_1;
CREATE TABLE ignore_cols_1 ( f_0 smallint, f_1 smallint DEFAULT NULL, f_2 smallint DEFAULT NULL, f_3 smallint DEFAULT NULL, PRIMARY KEY (f_0) );
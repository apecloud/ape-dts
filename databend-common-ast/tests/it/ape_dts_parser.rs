use std::fs::File;
use std::io::Write;

use databend_common_ast::parser::*;
use goldenfile::Mint;

fn run_ddl_test(cases: &[&str], dialect: Dialect, file: &mut File) {
    for case in cases {
        let src = unindent::unindent(case);
        let src = src.trim();
        let tokens = tokenize_sql(src).unwrap();
        let (stmt, fmt) = parse_sql(&tokens, dialect).unwrap();

        writeln!(file, "---------- Input ----------").unwrap();
        writeln!(file, "{}", src).unwrap();
        writeln!(file, "---------- Output ---------").unwrap();
        writeln!(file, "{}", stmt).unwrap();
        writeln!(file, "---------- AST ------------").unwrap();
        writeln!(file, "{:#?}", stmt).unwrap();
        writeln!(file, "\n").unwrap();
        if fmt.is_some() {
            writeln!(file, "---------- FORMAT ------------").unwrap();
            writeln!(file, "{:#?}", fmt).unwrap();
        }
    }
}

#[test]
fn test_mysql_ddl_statement() {
    let mut mint = Mint::new("tests/it/ape-dts-testdata");
    let file = &mut mint.new_goldenfile("mysql-ddl.txt").unwrap();
    // let file = &mut File::create("tests/it/ape-dts-testdata/mysql-ddl.txt").unwrap();
    let cases = &[
        // test_create_table_with_schema_mysql
        // schema.table
        "create table aaa.bbb (id int)",
        // escapes
        "create table `aaa`.`bbb` (id int)",
        // spaces
        "  create   table  aaa . bbb   (id int)  ",
        // spaces + escapes
        "  create   table  `aaa` . `bbb`   (id int)  ",
        // if not exists
        "create table if  not  exists `aaa`.`bbb` (id int)",
        // comments
        "create /*some comments,*/table/*some comments*/ `aaa`.`bbb` (id int)",
        // escapes + spaces + if not exists + comments
        "create /*some comments,*/table/*some comments*/ if  not  exists  `aaa` .  `bbb` (id int)  ",

        // test_create_table_with_schema_with_special_characters_mysql
        // schema with special characters
        "CREATE TABLE IF NOT EXISTS `test_db_*.*`.bbb(id int);",
        "CREATE TABLE IF NOT EXISTS `中文.others*&^%$#@!+_)(&^%#`.`中文!@$#$%^&*&(_+)`(id int);",

        // test_create_table_without_schema_mysql
        // schema.table
        "create table bbb (id int)",
        // escapes
        "create table `bbb` (id int)",
        // spaces
        "  create   table  bbb   (id int)  ",
        // spaces + escapes
        "  create   table   `bbb`   (id int)  ",
        // if not exists
        "create table if  not  exists `bbb` (id int)",
        // comments
        "create /*some comments,*/table/*some comments*/ `bbb` (id int)",
        //  escapes + spaces + if not exists + comments
        "create /*some comments,*/table/*some comments*/ if  not  exists    `bbb` (id int)  ",

        // test_drop_table_with_schema_mysql
        // schema.table
        "drop table aaa.bbb",
        // escapes
        "drop table `aaa`.`bbb`",
        // spaces
        "  drop   table  aaa . bbb  ",
        // spaces + escapes
        "  drop   table  `aaa` . `bbb`  ",
        // if exists
        "drop table if  exists `aaa`.`bbb`",
        // comments
        "drop /*some comments,*/table/*some comments*/ `aaa`.`bbb`",
        //  escapes + spaces + if exists + comments
        "drop /*some comments,*/table/*some comments*/ if  exists  `aaa` .  `bbb`  ",

        // test_drop_table_without_schema_mysql
        // schema.table
        "drop table bbb",
        // escapes
        "drop table `bbb`",
        // spaces
        "  drop   table   bbb  ",
        // spaces + escapes
        "  drop   table   `bbb`  ",
        // if exists
        "drop table if  exists `bbb`",
        // comments
        "drop /*some comments,*/table/*some comments*/ `bbb`",
        //  escapes + spaces + if exists + comments
        "drop /*some comments,*/table/*some comments*/ if  exists    `bbb`  ",

        // test_alter_table_with_schema_mysql
        // schema.table
        "alter table aaa.bbb add column value int",
        // escapes
        "alter table `aaa`.`bbb` add column value int",
        // spaces
        "  alter   table  aaa . bbb   add column value int",
        // spaces + escapes
        "  alter   table  `aaa` . `bbb`   add column value int",
        // if exists
        "alter table `aaa`.`bbb` add column value int",
        // comments
        "alter /*some comments,*/table/*some comments*/ `aaa`.`bbb` add column value int",
        //  escapes + spaces + comments
        "alter /*some comments,*/table/*some comments*/   `aaa` .  `bbb`   add column value int",

        // test_alter_table_without_schema_mysql
        // schema.table
        "alter table bbb add column value int",
        // escapes
        "alter table `bbb` add column value int",
        // spaces
        "  alter   table   bbb   add column value int",
        // spaces + escapes
        "  alter   table   `bbb`   add column value int",
        // comments
        "alter /*some comments,*/table/*some comments*/ `bbb` add column value int",
        //  escapes + spaces + comments
        "alter /*some comments,*/table/*some comments*/    `bbb`   add column value int",

        // test_alter_rename_table_mysql
        // TODO, these sqls are valid in MySQL but parser throws error: expecting `TO`
        // "ALTER TABLE tb_2 RENAME  tb_3",
        // "alter table tb_2 rename as tb_3",
        "alter table tb_2 rename to tb_3",
        // "ALTER TABLE `db_1`.tb_2 RENAME  `db_2`.tb_3",
        // "alter table `db_1`.tb_2 rename as `db_2`.tb_3",
        // "alter table `db_1`.tb_2 rename to `db_2`.tb_3",

        // test_create_database_mysql
        "create database aaa",
        // escapes
        "create database `aaa`",
        // spaces
        "  create   database   aaa",
        // spaces + escapes
        "  create   database   `aaa`  ",
        // if exists
        "create database if  not  exists `aaa`",
        // comments
        "create /*some comments,*/database/*some comments*/ `aaa`",
        //  escapes + spaces + if exists + comments
        "create /*some comments,*/database/*some comments*/ if  not  exists    `aaa`  ",

        // test_create_database_with_special_characters_mysql
        "CREATE DATABASE IF NOT EXISTS `test_db_*.*`;",
        "CREATE DATABASE IF NOT EXISTS `中文.others*&^%$#@!+_)(&^%#`;",

        // test_drop_database_mysql
        "drop database aaa",
        // escapes
        "drop database `aaa`",
        // spaces
        "  drop   database   aaa",
        // spaces + escapes
        "  drop   database   `aaa`  ",
        // if exists
        "drop database if  exists `aaa`",
        // comments
        "drop /*some comments,*/database/*some comments*/ `aaa`",
        //  escapes + spaces + if exists + comments
        "drop /*some comments,*/database/*some comments*/ if  exists    `aaa`  ",

        // test_alter_database_mysql, TODO
        // "alter database aaa CHARACTER SET utf8",
        // // escapes
        // "alter database `aaa` CHARACTER SET utf8",
        // // spaces
        // "  alter   database   aaa CHARACTER SET utf8",
        // // spaces + escapes
        // "  alter   database   `aaa`   CHARACTER SET utf8",
        // // comments
        // "alter /*some comments,*/database/*some comments*/ `aaa` CHARACTER SET utf8",
        // //  escapes + spaces + comments
        // "alter /*some comments,*/database/*some comments*/    `aaa`   CHARACTER SET utf8",

        // test_truncate_table_with_schema_mysql
        // schema.table
        "truncate table aaa.bbb",
        // escapes
        "truncate table `aaa`.`bbb`",
        // spaces
        "  truncate   table  aaa . bbb  ",
        // spaces + escapes
        "  truncate   table  `aaa` . `bbb`  ",
        // comments
        "truncate /*some comments,*/table/*some comments*/ `aaa`.`bbb`",
        //  escapes + spaces + comments
        "truncate /*some comments,*/table/*some comments*/   `aaa` .  `bbb`  ",
        // without keyword `table`
        // "truncate `aaa`.`bbb`",
        // "truncate /*some comments,*/table/*some comments*/ `aaa`.`bbb`",

        // test_truncate_table_without_schema_mysql
        // schema.table
        "truncate table bbb",
        // escapes
        "truncate table `bbb`",
        // spaces
        "  truncate   table   bbb  ",
        // spaces + escapes
        "  truncate   table   `bbb`  ",
        // comments
        "truncate /*some comments,*/table/*some comments*/ `bbb`",
        //  escapes + spaces + comments
        "truncate /*some comments,*/table/*some comments*/     `bbb`  ",

        // test_rename_table_mysql
        // schema.table
        "rename table aaa.bbb to aaa.ccc",
        // escapes
        "rename table `aaa`.`bbb` to aaa.ccc",
        // spaces
        "  rename   table  aaa . bbb   to aaa.ccc",
        // spaces + escapes
        "  rename   table  `aaa` . `bbb`   to aaa.ccc",
        // comments
        // "rename /*some comments,*/table/*some comments*/ `aaa`.`bbb` to aaa.ccc",
        // escapes + spaces + comments
        // "rename /*some comments,*/table/*some comments*/   `aaa` .  `bbb`   to aaa.ccc",
        // multiple tables + spaces + comments + multiple lines
        // r#"rename /*some comments,*/table/*some comments*/  
        // -- some comments2,
        // `aaa` .  `bbb`   to aaa.ccc, 
        // /*some comments3*/
        // bbb.ddd to eee.fff,  
        // -- some 中文注释, 
        // `中文` .  `中文😀`   to `中文😀`.`中文`"#,
        // without schema + multiple tables + spaces + comments + multiple lines
        // r#"rename /*some comments,*/table/*some comments*/  
        // -- some comments2,
        //   `bbb`   to ccc, 
        // /*some comments3*/
        // ddd to fff,  
        // -- some 中文注释, 
        //   `中文😀`   to `中文`"#,

        // test_create_index_mysql
        // "create index idx2 on t1 ((col1 + col2), (col1 - col2), col1);",
        // "create unique index `idx2` using  btree  on `d1`.`t1`((col1 + col2), (col1 - col2), col1);",

        // test_drop_index_mysql
        // "drop index index1 on t1 algorithm=default;",
        // // escapes
        // "drop index `index1` on `d1`.`t1` algorithm=default;",
    ];
    run_ddl_test(cases, Dialect::MySQL, file);
}

#[test]
fn test_pg_ddl_statement() {
    let mut mint = Mint::new("tests/it/ape-dts-testdata");
    let file = &mut mint.new_goldenfile("pg-ddl.txt").unwrap();
    // let file = &mut File::create("tests/it/ape-dts-testdata/pg-ddl.txt").unwrap();
    let cases = &[
        // test_create_table_multi_lines_pg
        r#"CREATE TABLE -- some comments
            IF NOT EXISTS 
            db_1.tb_1 
            (id int,
            value int);"#,
        // test_create_table_with_schema_with_upper_case_pg
        r#"CREATE TABLE IF NOT EXISTS Test_DB.Test_TB(id int, "Value" int);"#,
        r#"CREATE TABLE IF NOT EXISTS "Test_DB".Test_TB(id int, "Value" int);"#,
        r#"CREATE TABLE IF NOT EXISTS "Test_DB"."Test_TB"(id int, "Value" int);"#,
        // test_create_table_with_schema_with_special_characters_pg
        r#"CREATE TABLE IF NOT EXISTS "test_db_*.*".bbb(id int);"#,
        r#"CREATE TABLE IF NOT EXISTS "中文.others*&^%$#@!+_)(&^%#"."中文!@$#$%^&*&(_+)"(id int);"#,
        // test_create_table_with_temporary_pg
        // r#"create UNLOGGED table tb_1(ts TIMESTAMP);"#,
        r#"create TEMPORARY table tb_2(ts TIMESTAMP);"#,
        r#"create temp table tb_3(ts TIMESTAMP);"#,
        // r#"create GLOBAL TEMPORARY table tb_4(ts TIMESTAMP) ON COMMIT DELETE ROWS;"#,
        // r#"create local temp table tb_5(ts TIMESTAMP);"#,
        // test_alter_table_with_schema_pg
        // escapes + spaces + comments
        r#"alter /*some comments,*/table/*some comments*/   "aaa" .  "bbb"   add column value int"#,
        // escapes + spaces + if exists + only + comments
        // r#"alter /*some comments,*/table
        // if exists
        // only
        // -- some commets
        // "aaa" .  "bbb"
        // add column
        // value int"#,
        // test_alter_table_without_schema_pg
        // schema.table
        "alter table bbb add column value int",
        // escapes
        "alter table `bbb` add column value int",
        // spaces
        "  alter   table   bbb   add column value int",
        // spaces + escapes
        "  alter   table   `bbb`   add column value int",
        // comments
        "alter /*some comments,*/table/*some comments*/ `bbb` add column value int",
        //  escapes + spaces + comments
        "alter /*some comments,*/table/*some comments*/    `bbb`   add column value int",
        // test_alter_rename_table_pg
        "ALTER TABLE tb_1 RENAME TO tb_2",
        "alter table tb_1 rename to tb_2",
        // r#"ALTER TABLE IF EXISTS ONLY "schema_1".tb_1 RENAME TO tb_2"#,
        r#"alter table "schema_1".tb_1 rename to tb_2"#,
        // r#"ALTER TABLE IF EXISTS ONLY "schema_1".tb_1 SET SCHEMA tb_2"#,
        // r#"alter table "schema_1".tb_1 set schema tb_2"#,
        // test_create_schema_pg
        // "create schema aaa",
        // // escapes
        // "create schema \"aaa\"",
        // // spaces
        // "  create   schema   aaa",
        // // spaces + escapes
        // "  create   schema   \"aaa\"  ",
        // // if exists
        // "create schema if  not  exists \"aaa\"",
        // // comments
        // "create /*some comments,*/schema/*some comments*/ \"aaa\"",
        // //  escapes + spaces + if exists + comments
        // "create /*some comments,*/schema/*some comments*/ if  not  exists    \"aaa\"  ",
        // // test_create_schema_with_special_characters_pg
        // "CREATE SCHEMA IF NOT EXISTS \"test_db_*.*\";",
        // "CREATE SCHEMA IF NOT EXISTS \"中文.others*&^%$#@!+_)(&^%#\";",
        // // test_drop_schema_pg
        // "drop schema aaa",
        // // escapes
        // "drop schema \"aaa\"",
        // // spaces
        // "  drop   schema   aaa",
        // // spaces + escapes
        // "  drop   schema   \"aaa\"  ",
        // // if exists
        // "drop schema if  exists \"aaa\"",
        // // comments
        // "drop /*some comments,*/schema/*some comments*/ \"aaa\"",
        // //  escapes + spaces + if exists + comments
        // "drop /*some comments,*/schema/*some comments*/ if  exists    \"aaa\"  ",
        // test_alter_schema_pg
        // "alter schema aaa rename to bbb",
        // escapes
        // "alter schema \"aaa\" rename to bbb",
        // spaces
        // "  alter   schema   aaa rename to bbb",
        // spaces + escapes
        // "  alter   schema   \"aaa\"   rename to bbb",
        // comments
        // "alter /*some comments,*/schema/*some comments*/ \"aaa\" rename to bbb",
        //  escapes + spaces + comments
        // "alter /*some comments,*/schema/*some comments*/    \"aaa\"   rename to bbb",
        // test_truncate_table_pg
        // schema.table
        "truncate table aaa.bbb",
        // escapes + spaces + comments
        r#"truncate /*some comments,*/table/*some comments*/   "aaa" .  "bbb"  "#,
        // without keyword `table`
        // r#"truncate /*some comments,*/   "aaa" .  "bbb"  "#,
        // with keyword `only`
        // r#"truncate /*some comments,*/table/*some comments*/  ONLY "aaa"."bbb""#,
        // r#"truncate /*some comments,*/  ONLY "aaa"."bbb""#,
        // test_create_index_pg
        // r#"create index on "tb_1"(id);"#,
        // r#"create unique index
        //     concurrently -- some comments
        //     "idx3" on only "tb_1"(a);"#,
        // r#"create
        //     unique
        //     index
        //     concurrently -- some comments
        //     if not
        //     exists
        //     "idx3"
        //     on
        //     only
        //     "tb_1"(a);"#,
        // test_drop_index_pg
        // "drop index tb_1_id_idx",
        // r#"drop index if exists tb_1_id_idx,tb_1_id_idx1 RESTRICT;"#,
        // r#"drop index CONCURRENTLY if exists tb_1_id_idx3 RESTRICT;"#,
    ];
    run_ddl_test(cases, Dialect::PostgreSQL, file);
}

use std::fmt::Debug;
use std::fmt::Display;
use std::fs::File;
use std::io::Write;

use databend_common_ast::ast::Statement;
use databend_common_ast::parser::*;
use goldenfile::Mint;

// fn run_parser<P, O>(file: &mut dyn Write, parser: P, src: &str)
// where
//     P: FnMut(Input) -> IResult<O>,
//     O: Debug + Display,
// {
//     run_parser_with_dialect(file, parser, Dialect::PostgreSQL, ParseMode::Default, src)
// }

#[test]
fn test_raw_insert_stmt() {
    // let mut mint = Mint::new("tests/it/ape_dts_testdata");
    // let file = &mut mint.new_goldenfile("mysql-ddl.txt").unwrap();

    let mut file =
        File::create("tests/it/ape_dts_testdata/mysql-ddl.txt").expect("Failed to create file");

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

        // test_create_table_with_schema_with_special_characters
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
        "ALTER TABLE tb_2 RENAME  tb_3",
        "alter table tb_2 rename as tb_3",
        "alter table tb_2 rename to tb_3",
        "ALTER TABLE `db_1`.tb_2 RENAME  `db_2`.tb_3",
        "alter table `db_1`.tb_2 rename as `db_2`.tb_3",
        "alter table `db_1`.tb_2 rename to `db_2`.tb_3",

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

        // test_alter_database_mysql
        "alter database aaa CHARACTER SET utf8",
        // escapes
        "alter database `aaa` CHARACTER SET utf8",
        // spaces
        "  alter   database   aaa CHARACTER SET utf8",
        // spaces + escapes
        "  alter   database   `aaa`   CHARACTER SET utf8",
        // comments
        "alter /*some comments,*/database/*some comments*/ `aaa` CHARACTER SET utf8",
        //  escapes + spaces + comments
        "alter /*some comments,*/database/*some comments*/    `aaa`   CHARACTER SET utf8",

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
        "truncate `aaa`.`bbb`",
        "truncate /*some comments,*/table/*some comments*/ `aaa`.`bbb`",

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
        "rename /*some comments,*/table/*some comments*/ `aaa`.`bbb` to aaa.ccc",
        // escapes + spaces + comments
        "rename /*some comments,*/table/*some comments*/   `aaa` .  `bbb`   to aaa.ccc",
        // multiple tables + spaces + comments + multiple lines
        r#"rename /*some comments,*/table/*some comments*/  
        -- some comments2,
        `aaa` .  `bbb`   to aaa.ccc, 
        /*some comments3*/
        bbb.ddd to eee.fff,  
        -- some 中文注释, 
        `中文` .  `中文😀`   to `中文😀`.`中文`"#,
        // without schema + multiple tables + spaces + comments + multiple lines
        r#"rename /*some comments,*/table/*some comments*/  
        -- some comments2,
          `bbb`   to ccc, 
        /*some comments3*/
        ddd to fff,  
        -- some 中文注释, 
          `中文😀`   to `中文`"#,

        // test_create_index_mysql
        "create index idx2 on t1 ((col1 + col2), (col1 - col2), col1);",
        "create unique index `idx2` using  btree  on `d1`.`t1`((col1 + col2), (col1 - col2), col1);",

        // test_drop_index_mysql
        "drop index index1 on t1 algorithm=default;",
        // escapes
        "drop index `index1` on `d1`.`t1` algorithm=default;",
    ];

    for case in cases {
        let src = unindent::unindent(case);
        let src = src.trim();
        let tokens = tokenize_sql(src).unwrap();
        let (stmt, fmt) = parse_sql(&tokens, Dialect::MySQL).unwrap();

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

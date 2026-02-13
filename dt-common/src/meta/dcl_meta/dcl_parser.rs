use crate::config::config_enums::DbType;
use crate::error::Error;
use crate::meta::dcl_meta::dcl_data::DclData;
use crate::meta::dcl_meta::dcl_statement::{DclStatement, OriginStatement};
use crate::meta::dcl_meta::dcl_type::DclType;
use anyhow::bail;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::{multispace0, multispace1};
use nom::sequence::tuple;
use nom::IResult;
use regex::Regex;
use std::borrow::Cow;

pub struct DclParser {
    db_type: DbType,
}

pub type DclSchemaTable = (Option<Vec<u8>>, Vec<u8>);

impl DclParser {
    pub fn new(db_type: DbType) -> Self {
        Self { db_type }
    }

    pub fn parse(&self, sql: &str) -> anyhow::Result<Option<DclData>> {
        let sql = Self::remove_comments(sql);

        if !Self::dcl_simple_judgment(&sql) {
            return Ok(None);
        }

        let input = sql.trim().as_bytes();
        match self.sql_query(input) {
            Ok((_, mut dcl)) => {
                dcl.db_type = self.db_type.clone();
                Ok(Some(dcl))
            }
            Err(err) => {
                let error = match err {
                    nom::Err::Incomplete(_) => "incomplete".to_string(),
                    nom::Err::Error(e) | nom::Err::Failure(e) => {
                        format!("code: {:?}, input: {}", e.code, to_string(e.input))
                    }
                };
                bail! {Error::Unexpected(format!("failed to parse sql: {}, error: {}", sql, error))}
            }
        }
    }

    fn remove_comments(sql: &str) -> Cow<str> {
        // "create /*some comments,*/table/*some comments*/ `aaa`.`bbb`"
        let regex = Regex::new(r"(/\*([^*]|\*+[^*/*])*\*+/)|(--[^\n]*\n)").unwrap();
        regex.replace_all(sql, "")
    }

    fn dcl_simple_judgment(sql: &str) -> bool {
        let sql_lowercase = sql.to_lowercase();
        !sql_lowercase.trim_start().starts_with("insert into ")
            && !sql_lowercase.trim_start().starts_with("update ")
            && !sql_lowercase.trim_start().starts_with("delete ")
            && !sql_lowercase.trim_start().starts_with("replace into ")
    }

    fn sql_query<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DclData> {
        alt((
            |i| self.create_user(i),
            |i| self.alter_user(i),
            |i| self.create_role(i),
            |i| self.drop_user(i),
            |i| self.drop_role(i),
            |i| self.grant(i),
            |i| self.revoke(i),
            |i| self.set_role(i),
        ))(i)
    }

    fn create_user<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DclData> {
        let _ = tuple((
            tag_no_case("create"),
            multispace1,
            tag_no_case("user"),
            multispace0,
        ))(i)?;

        let statement = OriginStatement {
            origin: to_string(i),
        };

        let dcl = DclData {
            dcl_type: DclType::CreateUser,
            statement: DclStatement::CreateUser(statement),
            ..Default::default()
        };
        Ok((&[], dcl))
    }

    fn alter_user<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DclData> {
        let _ = tuple((
            tag_no_case("alter"),
            multispace1,
            tag_no_case("user"),
            multispace0,
        ))(i)?;

        let statement = OriginStatement {
            origin: to_string(i),
        };

        let dcl = DclData {
            dcl_type: DclType::AlterUser,
            statement: DclStatement::AlterUser(statement),
            ..Default::default()
        };
        Ok((&[], dcl))
    }

    fn create_role<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DclData> {
        let _ = tuple((
            tag_no_case("create"),
            multispace1,
            tag_no_case("role"),
            multispace0,
        ))(i)?;

        let statement = OriginStatement {
            origin: to_string(i),
        };

        let dcl = DclData {
            dcl_type: DclType::CreateRole,
            statement: DclStatement::CreateRole(statement),
            ..Default::default()
        };
        Ok((&[], dcl))
    }

    fn drop_user<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DclData> {
        let _ = tuple((
            tag_no_case("drop"),
            multispace1,
            tag_no_case("user"),
            multispace0,
        ))(i)?;

        let statement = OriginStatement {
            origin: to_string(i),
        };

        let dcl = DclData {
            dcl_type: DclType::DropUser,
            statement: DclStatement::DropUser(statement),
            ..Default::default()
        };
        Ok((&[], dcl))
    }

    fn drop_role<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DclData> {
        let _ = tuple((
            tag_no_case("drop"),
            multispace1,
            tag_no_case("role"),
            multispace0,
        ))(i)?;

        let statement = OriginStatement {
            origin: to_string(i),
        };

        let dcl = DclData {
            dcl_type: DclType::DropRole,
            statement: DclStatement::DropRole(statement),
            ..Default::default()
        };
        Ok((&[], dcl))
    }

    fn grant<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DclData> {
        let (remaining_input, _) = tuple((tag_no_case("grant"), multispace1))(i)?;

        let statement = OriginStatement {
            origin: to_string(i),
        };

        let dcl = DclData {
            dcl_type: DclType::Grant,
            statement: DclStatement::Grant(statement),
            db_type: self.db_type.clone(),
            default_schema: "".to_string(),
        };
        Ok((remaining_input, dcl))
    }

    fn revoke<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DclData> {
        let (remaining_input, _) = tuple((tag_no_case("revoke"), multispace1))(i)?;

        let statement = OriginStatement {
            origin: to_string(i),
        };

        let dcl = DclData {
            dcl_type: DclType::Revoke,
            statement: DclStatement::Revoke(statement),
            db_type: self.db_type.clone(),
            default_schema: "".to_string(),
        };
        Ok((remaining_input, dcl))
    }

    fn set_role<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DclData> {
        let _ = tuple((
            tag_no_case("set"),
            multispace1,
            tag_no_case("default"),
            multispace1,
            tag_no_case("role"),
        ))(i)?;

        let statement = OriginStatement {
            origin: to_string(i),
        };

        let dcl = DclData {
            dcl_type: DclType::SetRole,
            statement: DclStatement::SetRole(statement),
            ..Default::default()
        };
        Ok((&[], dcl))
    }
}

fn to_string(i: &[u8]) -> String {
    String::from_utf8_lossy(i).to_string()
}

#[cfg(test)]
mod tests {

    use super::*;

    fn create_mysql_parser() -> DclParser {
        DclParser::new(DbType::Mysql)
    }

    fn check_internal(expect_sqls: Vec<&str>, not_expect_sqls: Vec<&str>, dcl_type: DclType) {
        let parser = create_mysql_parser();
        for sql in expect_sqls {
            let result = parser.parse(sql).unwrap().unwrap();
            assert_eq!(result.dcl_type, dcl_type);
        }
        for sql in not_expect_sqls {
            match parser.parse(sql) {
                Ok(Some(dcl_data)) => {
                    assert_ne!(dcl_data.dcl_type, dcl_type);
                }
                Ok(None) => {
                    panic!("Expected DCL data but got None");
                }
                Err(_) => {}
            };
        }
    }

    #[test]
    fn test_mysql_create_user() {
        let sqls = vec![
            // basic
            "CREATE USER 'user1'@'localhost' IDENTIFIED BY 'password123'",
            // comment
            "CREATE /*comment1*/ USER /*comment2*/ 'user2'@'localhost' IDENTIFIED BY 'pass123'",
            // multi-line comment
            r#"CREATE /*multi-line
        comment*/ USER -- line comment
        'user3'@'localhost' IDENTIFIED BY 'pass123'"#,
            // case-insensitive
            "Create User 'USER4'@'localhost' IDENTIFIED BY 'pass123'",
            // multi-spaces and newlines
            r#"CREATE    USER    
        'user5'@'localhost'    
        IDENTIFIED    BY    'pass123'"#,
            // multi-users
            r#"CREATE USER 
        'user6'@'localhost' IDENTIFIED BY 'pass123',
        'user7'@'%' IDENTIFIED BY 'pass456'"#,
            // IF NOT EXISTS
            "CREATE USER IF NOT EXISTS 'user8'@'localhost' IDENTIFIED BY 'pass123'",
            // ssl
            r#"CREATE USER 'user9'@'localhost' 
        IDENTIFIED BY 'pass123' 
        REQUIRE SSL"#,
            // account lock
            "CREATE USER 'user10'@'localhost' IDENTIFIED BY 'pass123' ACCOUNT LOCK",
            // full options
            r#"CREATE USER IF NOT EXISTS 'user11'@'localhost'
        IDENTIFIED WITH mysql_native_password BY 'pass123'
        REQUIRE SSL
        PASSWORD EXPIRE INTERVAL 90 DAY
        FAILED_LOGIN_ATTEMPTS 3
        PASSWORD_LOCK_TIME 2
        ACCOUNT LOCK
        COMMENT 'test user'
        ATTRIBUTE '{"fname": "James", "lname": "Scott"}'"#,
        ];

        let not_expect_sqls = vec![
            r#"CREATE TABLE IF NOT EXISTS Test_DB.Test_TB(id int, "Value" int);"#,
            "ALTER USER 'test'@'localhost' IDENTIFIED BY 'new_password",
            "DROP USER 'test'@'localhost'",
        ];

        check_internal(sqls, not_expect_sqls, DclType::CreateUser);
    }

    #[test]
    fn test_mysql_alter_user() {
        let sqls = vec![
            // basic
            "ALTER USER 'user1'@'localhost' IDENTIFIED BY 'password123'",
            // comment
            "ALTER /*comment1*/ USER /*comment2*/ 'user2'@'localhost' IDENTIFIED BY 'pass123'",
            // multi-line comment
            r#"ALTER /*multi-line
        comment*/ USER -- line comment
        'user3'@'localhost' IDENTIFIED BY 'pass123'"#,
            // case-insensitive
            "Alter User 'USER4'@'localhost' IDENTIFIED BY 'pass123'",
            // multi-spaces and newlines
            r#"ALTER    USER    
        'user5'@'localhost'    
        IDENTIFIED    BY    'pass123'"#,
            // multi-users
            r#"ALTER USER 
        'user6'@'localhost' IDENTIFIED BY 'pass123',
        'user7'@'%' IDENTIFIED BY 'pass456'"#,
            // IF EXISTS
            "ALTER USER IF EXISTS 'user8'@'localhost' IDENTIFIED BY 'pass123'",
            // ssl
            r#"ALTER USER 'user9'@'localhost' 
        IDENTIFIED BY 'pass123' 
        REQUIRE SSL"#,
            // account lock
            "ALTER USER 'user10'@'localhost' IDENTIFIED BY 'pass123' ACCOUNT LOCK",
            // full options
            r#"ALTER USER IF EXISTS 'user11'@'localhost'
        IDENTIFIED WITH mysql_native_password BY 'pass123'
        REQUIRE SSL
        PASSWORD EXPIRE INTERVAL 90 DAY
        FAILED_LOGIN_ATTEMPTS 3
        PASSWORD_LOCK_TIME 2
        ACCOUNT LOCK
        COMMENT 'test user'
        ATTRIBUTE '{"fname": "James", "lname": "Scott"}'"#,
        ];

        let not_expect_sqls = vec![
            r#"CREATE TABLE IF NOT EXISTS Test_DB.Test_TB(id int, "Value" int);"#,
            "alter table aaa.bbb add column value int",
            "/*alter user*/alter table aaa.bbb add column value int",
        ];

        check_internal(sqls, not_expect_sqls, DclType::AlterUser);
    }

    #[test]
    fn test_mysql_create_role() {
        let sqls = vec![
            // basic
            "CREATE ROLE role1",
            // comment
            "CREATE /*comment1*/ ROLE /*comment2*/ role2",
            // multi-line comment
            r#"CREATE /*multi-line
        comment*/ ROLE -- line comment
        role3"#,
            // case-insensitive
            "Create Role ROLE4",
            // multi-spaces and newlines
            r#"CREATE    ROLE    
        role5"#,
            // multi-roles
            r#"CREATE ROLE 
        role6,
        role7"#,
            // IF NOT EXISTS
            "CREATE ROLE IF NOT EXISTS role8",
        ];

        let not_expect_sqls = vec![
            r#"CREATE TABLE IF NOT EXISTS Test_DB.Test_TB(id int, "Value" int);"#,
            "CREATE USER 'test'@'localhost' IDENTIFIED BY 'password'",
            "DROP ROLE role1",
        ];

        check_internal(sqls, not_expect_sqls, DclType::CreateRole);
    }

    #[test]
    fn test_mysql_drop_user() {
        let sqls = vec![
            // basic
            "DROP USER 'user1'@'localhost'",
            // comment
            "DROP /*comment1*/ USER /*comment2*/ 'user2'@'localhost'",
            // multi-line comment
            r#"DROP /*multi-line
        comment*/ USER -- line comment
        'user3'@'localhost'"#,
            // case-insensitive
            "Drop User 'USER4'@'localhost'",
            // multi-spaces and newlines
            r#"DROP    USER    
        'user5'@'localhost'"#,
            // multi-users
            r#"DROP USER 
        'user6'@'localhost',
        'user7'@'%'"#,
            // IF EXISTS
            "DROP USER IF EXISTS 'user8'@'localhost'",
        ];

        let not_expect_sqls = vec![
            r#"CREATE TABLE IF NOT EXISTS Test_DB.Test_TB(id int, "Value" int);"#,
            "CREATE USER 'test'@'localhost' IDENTIFIED BY 'password'",
            "ALTER USER 'test'@'localhost' IDENTIFIED BY 'new_password'",
        ];

        check_internal(sqls, not_expect_sqls, DclType::DropUser);
    }

    #[test]
    fn test_mysql_drop_role() {
        let sqls = vec![
            // basic
            "DROP ROLE role1",
            // comment
            "DROP /*comment1*/ ROLE /*comment2*/ role2",
            // multi-line comment
            r#"DROP /*multi-line
        comment*/ ROLE -- line comment
        role3"#,
            // case-insensitive
            "Drop Role ROLE4",
            // multi-spaces and newlines
            r#"DROP    ROLE    
        role5"#,
            // multi-roles
            r#"DROP ROLE 
        role6,
        role7"#,
            // IF EXISTS
            "DROP ROLE IF EXISTS role8",
        ];

        let not_expect_sqls = vec![
            r#"CREATE TABLE IF NOT EXISTS Test_DB.Test_TB(id int, "Value" int);"#,
            "CREATE ROLE role1",
            "DROP USER 'test'@'localhost'",
        ];

        check_internal(sqls, not_expect_sqls, DclType::DropRole);
    }

    #[test]
    fn test_mysql_grant() {
        let sqls = vec![
            // basic
            "GRANT ALL ON db.* TO 'user1'@'localhost'",
            // comment
            "GRANT /*comment1*/ ALL /*comment2*/ ON db.* TO 'user2'@'localhost'",
            // multi-line comment
            r#"GRANT /*multi-line
        comment*/ ALL -- line comment
        ON db.* TO 'user3'@'localhost'"#,
            // case-insensitive
            "Grant All On db.* To 'USER4'@'localhost'",
            // multi-spaces and newlines
            r#"GRANT    ALL    ON    
        db.*    TO    'user5'@'localhost'"#,
            // specific privileges
            "GRANT SELECT, INSERT, UPDATE ON db.table TO 'user6'@'localhost'",
            // with grant option
            "GRANT ALL ON *.* TO 'user7'@'localhost' WITH GRANT OPTION",
        ];

        let not_expect_sqls = vec![
            r#"CREATE TABLE IF NOT EXISTS Test_DB.Test_TB(id int, "Value" int);"#,
            "CREATE USER 'test'@'localhost' IDENTIFIED BY 'password'",
            "REVOKE ALL ON db.* FROM 'test'@'localhost'",
        ];

        check_internal(sqls, not_expect_sqls, DclType::Grant);
    }

    #[test]
    fn test_mysql_revoke() {
        let sqls = vec![
            // basic
            "REVOKE ALL ON db.* FROM 'user1'@'localhost'",
            // comment
            "REVOKE /*comment1*/ ALL /*comment2*/ ON db.* FROM 'user2'@'localhost'",
            // multi-line comment
            r#"REVOKE /*multi-line
        comment*/ ALL -- line comment
        ON db.* FROM 'user3'@'localhost'"#,
            // case-insensitive
            "Revoke All On db.* From 'USER4'@'localhost'",
            // multi-spaces and newlines
            r#"REVOKE    ALL    ON    
        db.*    FROM    'user5'@'localhost'"#,
            // specific privileges
            "REVOKE SELECT, INSERT, UPDATE ON db.table FROM 'user6'@'localhost'",
            // grant option
            "REVOKE GRANT OPTION ON *.* FROM 'user7'@'localhost'",
        ];

        let not_expect_sqls = vec![
            r#"CREATE TABLE IF NOT EXISTS Test_DB.Test_TB(id int, "Value" int);"#,
            "CREATE USER 'test'@'localhost' IDENTIFIED BY 'password'",
            "GRANT ALL ON db.* TO 'test'@'localhost'",
        ];

        check_internal(sqls, not_expect_sqls, DclType::Revoke);
    }

    #[test]
    fn test_mysql_set_role() {
        let sqls = vec![
            // basic
            "SET DEFAULT ROLE role1 TO 'user1'@'localhost'",
            // comment
            "SET /*comment1*/ DEFAULT /*comment2*/ ROLE role2 TO 'user2'@'localhost'",
            // multi-line comment
            r#"SET /*multi-line
        comment*/ DEFAULT -- line comment
        ROLE role3 TO 'user3'@'localhost'"#,
            // case-insensitive
            "Set Default Role ROLE4 To 'USER4'@'localhost'",
            // multi-spaces and newlines
            r#"SET    DEFAULT    ROLE    
        role5    TO    'user5'@'localhost'"#,
            // multi-roles
            r#"SET DEFAULT ROLE 
        role6, role7 TO 'user8'@'localhost'"#,
            // ALL
            "SET DEFAULT ROLE ALL TO 'user9'@'localhost'",
            // NONE
            "SET DEFAULT ROLE NONE TO 'user10'@'localhost'",
        ];

        let not_expect_sqls = vec![];

        check_internal(sqls, not_expect_sqls, DclType::SetRole);
    }
}

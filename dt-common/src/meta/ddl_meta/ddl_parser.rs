use std::{
    borrow::Cow,
    str::{self},
};

use anyhow::bail;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::{
        complete::{multispace0, multispace1},
        is_alphanumeric,
    },
    combinator::{map, not, opt, peek, recognize},
    multi::many1,
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};
use regex::Regex;

use super::{
    ddl_data::DdlData,
    ddl_statement::{
        AlterDatabaseStatement, AlterSchemaStatement, CreateDatabaseStatement,
        CreateSchemaStatement, DdlStatement, DropDatabaseStatement, DropMultiTableStatement,
        DropSchemaStatement, MssqlCreateDatabaseStatement, MssqlCreateIndexStatement,
        MssqlCreateSchemaStatement, MssqlCreateTableStatement, MssqlDropDatabaseStatement,
        MssqlDropIndexStatement, MssqlDropSchemaStatement, MssqlDropTableStatement,
        MysqlAlterTableRenameStatement, MysqlAlterTableStatement, MysqlCreateIndexStatement,
        MysqlCreateTableStatement, MysqlDropIndexStatement, MysqlTruncateTableStatement,
        PgAlterTableRenameStatement, PgAlterTableSetSchemaStatement, PgAlterTableStatement,
        PgCreateIndexStatement, PgCreateTableStatement, PgDropMultiIndexStatement,
        PgTruncateTableStatement, RenameMultiTableStatement,
    },
    ddl_type::DdlType,
    keywords::{
        keyword_a_to_c, keyword_c_to_e, keyword_e_to_i, keyword_i_to_o, keyword_o_to_s,
        keyword_s_to_z,
    },
};
use crate::error::DtError;
use crate::{config::config_enums::DbType, utils::sql_util::SqlUtil};

type SchemaTable = (Option<Vec<u8>>, Vec<u8>);

pub struct DdlParser {
    db_type: DbType,
}

impl DdlParser {
    pub fn new(db_type: DbType) -> Self {
        Self { db_type }
    }

    pub fn parse(&self, sql: &str) -> anyhow::Result<Option<DdlData>> {
        let sql = Self::remove_comments(sql);

        // In some cases, non-ddl statements may also enter this parse logic, such as heartbeat connections when mysql binlog_format=mix
        if !Self::ddl_simple_judgment(&sql) {
            return Ok(None);
        }

        let input = sql.trim().as_bytes();
        match self.sql_query(input) {
            Ok((_, mut ddl)) => {
                ddl.db_type = self.db_type.clone();
                Ok(Some(ddl))
            }
            Err(err) => {
                let error = match err {
                    nom::Err::Incomplete(_) => "incomplete".to_string(),
                    nom::Err::Error(e) | nom::Err::Failure(e) => {
                        format!("code: {:?}, input: {}", e.code, to_string(e.input))
                    }
                };
                bail! {DtError::StatementFailed(format!("failed to parse sql: {}, error: {}", sql, error))}
            }
        }
    }

    fn remove_comments(sql: &str) -> Cow<str> {
        // "create /*some comments,*/table/*some comments*/ `aaa`.`bbb`"
        match Regex::new(r"(/\*([^*]|\*+[^*/*])*\*+/)|(--[^\n]*\n)") {
            Ok(regex) => regex.replace_all(sql, ""),
            Err(_) => Cow::Borrowed(sql),
        }
    }

    fn ddl_simple_judgment(sql: &str) -> bool {
        let sql_lowercase = sql.to_lowercase();
        !sql_lowercase.trim_start().starts_with("insert into ")
            && !sql_lowercase.trim_start().starts_with("update ")
            && !sql_lowercase.trim_start().starts_with("delete ")
            && !sql_lowercase.trim_start().starts_with("replace into ")
    }

    /// parse ddl sql and return: (ddl_type, schema, table)
    fn sql_query<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        alt((
            |i| self.create_database(i),
            |i| self.drop_database(i),
            |i| self.alter_database(i),
            |i| self.create_schema(i),
            |i| self.drop_schema(i),
            |i| self.alter_schema(i),
            |i| self.create_table(i),
            |i| self.drop_table(i),
            |i| self.alter_table(i),
            |i| self.truncate_table(i),
            |i| self.rename_table(i),
            |i| self.create_index(i),
            |i| self.drop_index(i),
        ))(i)
    }

    fn create_database<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (remaining_input, (_, _, _, _, if_not_exists, database, _)) = tuple((
            tag_no_case("create"),
            multispace1,
            tag_no_case("database"),
            multispace1,
            opt(if_not_exists),
            |i| self.sql_identifier(i),
            multispace0,
        ))(i)?;

        let db = self.identifier_to_string(database);
        let unparsed = to_string(remaining_input);
        let statement = if matches!(self.db_type, DbType::Mssql) {
            DdlStatement::MssqlCreateDatabase(MssqlCreateDatabaseStatement {
                db,
                if_not_exists: if_not_exists.is_some(),
                unparsed,
            })
        } else {
            DdlStatement::CreateDatabase(CreateDatabaseStatement {
                db,
                if_not_exists: if_not_exists.is_some(),
                unparsed,
            })
        };

        let ddl = DdlData {
            ddl_type: DdlType::CreateDatabase,
            statement,
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn drop_database<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (remaining_input, (_, _, _, _, if_exists, database, _)) = tuple((
            tag_no_case("drop"),
            multispace1,
            tag_no_case("database"),
            multispace1,
            opt(if_exists),
            |i| self.sql_identifier(i),
            multispace0,
        ))(i)?;

        let db = self.identifier_to_string(database);
        let unparsed = to_string(remaining_input);
        let statement = if matches!(self.db_type, DbType::Mssql) {
            DdlStatement::MssqlDropDatabase(MssqlDropDatabaseStatement {
                db,
                if_exists: if_exists.is_some(),
                unparsed,
            })
        } else {
            DdlStatement::DropDatabase(DropDatabaseStatement {
                db,
                if_exists: if_exists.is_some(),
                unparsed,
            })
        };

        let ddl = DdlData {
            ddl_type: DdlType::DropDatabase,
            statement,
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn alter_database<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (remaining_input, (_, _, _, _, database, _)) = tuple((
            tag_no_case("alter"),
            multispace1,
            tag_no_case("database"),
            multispace1,
            |i| self.sql_identifier(i),
            multispace1,
        ))(i)?;

        let statement = AlterDatabaseStatement {
            db: self.identifier_to_string(database),
            unparsed: to_string(remaining_input),
        };

        let ddl = DdlData {
            ddl_type: DdlType::AlterDatabase,
            statement: DdlStatement::AlterDatabase(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn create_schema<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (remaining_input, (_, _, _, _, if_not_exists, authorization, schema, _)) =
            tuple((
                tag_no_case("create"),
                multispace1,
                tag_no_case("schema"),
                multispace1,
                opt(if_not_exists),
                opt(tuple((tag_no_case("authorization"), multispace1))),
                |i| self.sql_identifier(i),
                multispace0,
            ))(i)?;

        let schema = self.identifier_to_string(schema);
        let unparsed = to_string(remaining_input);
        let statement = if matches!(self.db_type, DbType::Mssql) {
            DdlStatement::MssqlCreateSchema(MssqlCreateSchemaStatement {
                db: String::new(),
                schema,
                authorization: authorization.is_some(),
                unparsed,
            })
        } else {
            DdlStatement::CreateSchema(CreateSchemaStatement {
                schema,
                if_not_exists: if_not_exists.is_some(),
                authorization: authorization.is_some(),
                unparsed,
            })
        };

        let ddl = DdlData {
            ddl_type: DdlType::CreateSchema,
            statement,
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn drop_schema<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (remaining_input, (_, _, _, _, if_exists, schema, _)) = tuple((
            tag_no_case("drop"),
            multispace1,
            tag_no_case("schema"),
            multispace1,
            opt(if_exists),
            |i| self.sql_identifier(i),
            multispace0,
        ))(i)?;

        let schema = self.identifier_to_string(schema);
        let unparsed = to_string(remaining_input);
        let statement = if matches!(self.db_type, DbType::Mssql) {
            DdlStatement::MssqlDropSchema(MssqlDropSchemaStatement {
                db: String::new(),
                schema,
                if_exists: if_exists.is_some(),
                unparsed,
            })
        } else {
            DdlStatement::DropSchema(DropSchemaStatement {
                schema,
                if_exists: if_exists.is_some(),
                unparsed,
            })
        };

        let ddl = DdlData {
            ddl_type: DdlType::DropSchema,
            statement,
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn alter_schema<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let rename_to = |i: &'a [u8]| -> IResult<&'a [u8], &[u8]> {
            let (remaining_input, (_, _, _, _, new_name, _)) = tuple((
                tag_no_case("rename"),
                multispace1,
                tag_no_case("to"),
                multispace1,
                |i| self.sql_identifier(i),
                multispace0,
            ))(i)?;
            Ok((remaining_input, new_name))
        };

        let (remaining_input, (_, _, _, _, schema, _, rename_to, _)) = tuple((
            tag_no_case("alter"),
            multispace1,
            tag_no_case("schema"),
            multispace1,
            |i| self.sql_identifier(i),
            multispace1,
            opt(rename_to),
            multispace0,
        ))(i)?;

        let mut statement = AlterSchemaStatement {
            schema: self.identifier_to_string(schema),
            new_schema: None,
            unparsed: to_string(remaining_input),
        };
        if let Some(new_schema) = rename_to {
            statement.new_schema = Some(self.identifier_to_string(new_schema));
        }

        let ddl = DdlData {
            ddl_type: DdlType::AlterSchema,
            statement: DdlStatement::AlterSchema(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn create_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        match self.db_type {
            DbType::Pg => self.pg_create_table(i),
            DbType::Mssql => self.mssql_create_table(i),
            _ => self.mysql_create_table(i),
        }
    }

    fn mysql_create_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (remaining_input, (_, _, _, _, _, if_not_exists, table, _)) = tuple((
            tag_no_case("create"),
            multispace1,
            opt(tuple((tag_no_case("temporary"), multispace1))),
            tag_no_case("table"),
            multispace1,
            opt(if_not_exists),
            |i| self.schema_table(i),
            multispace0,
        ))(i)?;

        // temporary tables won't be in binlog
        let (schema, tb) = self.parse_table(table);
        let statement = MysqlCreateTableStatement {
            db: schema,
            tb,
            if_not_exists: if_not_exists.is_some(),
            unparsed: to_string(remaining_input),
        };

        let ddl = DdlData {
            ddl_type: DdlType::CreateTable,
            statement: DdlStatement::MysqlCreateTable(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn pg_create_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        // https://www.postgresql.org/docs/16/sql-createtable.html
        let temporary = |i: &'a [u8]| -> IResult<&'a [u8], String> {
            let (remaining_input, (temporary_type, temporary, _)) = tuple((
                opt(tuple((
                    alt((tag_no_case("global"), tag_no_case("local"))),
                    multispace1,
                ))),
                alt((tag_no_case("temporary"), tag_no_case("temp"))),
                multispace1,
            ))(i)?;

            let res = if let Some(temporary_type) = temporary_type {
                format!("{} {}", to_string(temporary_type.0), to_string(temporary))
            } else {
                to_string(temporary)
            };
            Ok((remaining_input, res))
        };

        let unlogged = |i: &'a [u8]| -> IResult<&'a [u8], String> {
            let (remaining_input, (unloggd, _)) = tuple((tag_no_case("unlogged"), multispace1))(i)?;
            Ok((remaining_input, to_string(unloggd)))
        };

        let (remaining_input, (_, _, temporary_str, unlogged_str, _, _, if_not_exists, table, _)) =
            tuple((
                tag_no_case("create"),
                multispace1,
                opt(temporary),
                opt(unlogged),
                tag_no_case("table"),
                multispace1,
                opt(if_not_exists),
                |i| self.schema_table(i),
                multispace0,
            ))(i)?;

        // temporary tables won't be in binlog
        let (schema, tb) = self.parse_table(table);
        let statement = PgCreateTableStatement {
            schema,
            tb,
            if_not_exists: if_not_exists.is_some(),
            unparsed: to_string(remaining_input),
            temporary: temporary_str,
            unlogged: unlogged_str,
        };

        let ddl = DdlData {
            ddl_type: DdlType::CreateTable,
            statement: DdlStatement::PgCreateTable(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn mssql_create_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (remaining_input, (_, _, _, _, (db, schema, tb), _)) = tuple((
            tag_no_case("create"),
            multispace1,
            tag_no_case("table"),
            multispace1,
            |i| self.mssql_db_schema_table(i),
            multispace0,
        ))(i)?;

        let statement = MssqlCreateTableStatement {
            db,
            schema,
            tb,
            unparsed: to_string(remaining_input),
        };
        let ddl = DdlData {
            ddl_type: DdlType::CreateTable,
            statement: DdlStatement::MssqlCreateTable(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn drop_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        if self.db_type == DbType::Mssql {
            return self.mssql_drop_table(i);
        }

        let (remaining_input, (_, _, _, _, _, if_exists, table_list, _)) = tuple((
            tag_no_case("drop"),
            multispace1,
            opt(tuple((tag_no_case("temporary"), multispace1))),
            tag_no_case("table"),
            multispace1,
            opt(if_exists),
            |i| self.schema_table_list(i),
            multispace0,
        ))(i)?;

        let mut schema_tbs = Vec::new();
        for table in table_list {
            schema_tbs.push(self.parse_table(table))
        }

        let statement = DropMultiTableStatement {
            schema_tbs,
            if_exists: if_exists.is_some(),
            unparsed: to_string(remaining_input),
        };

        let ddl = DdlData {
            ddl_type: DdlType::DropTable,
            statement: DdlStatement::DropMultiTable(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn mssql_drop_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (remaining_input, (_, _, _, _, if_exists, (db, schema, tb), _)) = tuple((
            tag_no_case("drop"),
            multispace1,
            tag_no_case("table"),
            multispace1,
            opt(if_exists),
            |i| self.mssql_db_schema_table(i),
            multispace0,
        ))(i)?;

        let statement = MssqlDropTableStatement {
            db,
            schema,
            tb,
            if_exists: if_exists.is_some(),
            unparsed: to_string(remaining_input),
        };
        let ddl = DdlData {
            ddl_type: DdlType::DropTable,
            statement: DdlStatement::MssqlDropTable(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn alter_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        if self.db_type == DbType::Pg {
            self.pg_alter_table(i)
        } else {
            self.mysql_alter_table(i)
        }
    }

    fn mysql_alter_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        // https://dev.mysql.com/doc/refman/8.4/en/alter-table.html
        let rename_to = |i: &'a [u8]| -> IResult<&'a [u8], (String, String)> {
            let (remaining_input, (_, _, _, new_table, _)) = tuple((
                tag_no_case("rename"),
                multispace1,
                opt(tuple((
                    alt((tag_no_case("as"), tag_no_case("to"))),
                    multispace1,
                ))),
                |i| self.schema_table(i),
                multispace0,
            ))(i)?;
            Ok((remaining_input, self.parse_table(new_table)))
        };

        let (remaining_input, (_, _, _, _, table, _, rename_to, _)) = tuple((
            tag_no_case("alter"),
            multispace1,
            tag_no_case("table"),
            multispace1,
            |i| self.schema_table(i),
            multispace1,
            opt(rename_to),
            multispace0,
        ))(i)?;

        let (db, tb) = self.parse_table(table);
        if let Some((new_db, new_tb)) = rename_to {
            let statement = MysqlAlterTableRenameStatement {
                db,
                tb,
                new_db,
                new_tb,
                unparsed: to_string(remaining_input),
            };
            let ddl = DdlData {
                ddl_type: DdlType::AlterTable,
                statement: DdlStatement::MysqlAlterTableRename(statement),
                ..Default::default()
            };
            return Ok((remaining_input, ddl));
        }

        let statement = MysqlAlterTableStatement {
            db,
            tb,
            unparsed: to_string(remaining_input),
        };
        let ddl = DdlData {
            ddl_type: DdlType::AlterTable,
            statement: DdlStatement::MysqlAlterTable(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn pg_alter_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        // https://www.postgresql.org/docs/16/sql-altertable.html
        let rename_to = |i: &'a [u8]| -> IResult<&'a [u8], (String, String)> {
            let (remaining_input, (_, _, _, _, new_table, _)) = tuple((
                tag_no_case("rename"),
                multispace1,
                tag_no_case("to"),
                multispace1,
                |i| self.schema_table(i),
                multispace0,
            ))(i)?;
            Ok((remaining_input, self.parse_table(new_table)))
        };

        let set_schema = |i: &'a [u8]| -> IResult<&'a [u8], String> {
            let (remaining_input, (_, _, _, _, new_schema, _)) = tuple((
                tag_no_case("set"),
                multispace1,
                tag_no_case("schema"),
                multispace1,
                |i| self.sql_identifier(i),
                multispace0,
            ))(i)?;
            Ok((remaining_input, self.identifier_to_string(new_schema)))
        };

        let (
            remaining_input,
            (_, _, _, _, if_exists, only, table, _, rename_to_res, set_schema_res, _),
        ) = tuple((
            tag_no_case("alter"),
            multispace1,
            tag_no_case("table"),
            multispace1,
            opt(if_exists),
            opt(tuple((tag_no_case("only"), multispace1))),
            |i| self.schema_table(i),
            multispace1,
            opt(rename_to),
            opt(set_schema),
            multispace0,
        ))(i)?;

        let (schema, tb) = self.parse_table(table);
        if let Some((new_schema, new_tb)) = rename_to_res {
            let statement = PgAlterTableRenameStatement {
                schema,
                tb,
                new_schema,
                new_tb,
                if_exists: if_exists.is_some(),
                is_only: only.is_some(),
                unparsed: to_string(remaining_input),
            };
            let ddl = DdlData {
                ddl_type: DdlType::AlterTable,
                statement: DdlStatement::PgAlterTableRename(statement),
                ..Default::default()
            };
            return Ok((remaining_input, ddl));
        }

        if let Some(new_schema) = set_schema_res {
            let statement = PgAlterTableSetSchemaStatement {
                schema,
                tb: tb.clone(),
                new_schema,
                new_tb: tb,
                if_exists: if_exists.is_some(),
                is_only: only.is_some(),
                unparsed: to_string(remaining_input),
            };
            let ddl = DdlData {
                ddl_type: DdlType::AlterTable,
                statement: DdlStatement::PgAlterTableSetSchema(statement),
                ..Default::default()
            };
            return Ok((remaining_input, ddl));
        }

        let statement = PgAlterTableStatement {
            schema,
            tb,
            if_exists: if_exists.is_some(),
            is_only: only.is_some(),
            unparsed: to_string(remaining_input),
        };
        let ddl = DdlData {
            ddl_type: DdlType::AlterTable,
            statement: DdlStatement::PgAlterTable(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn truncate_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        if self.db_type == DbType::Pg {
            self.pg_truncate_table(i)
        } else {
            self.mysql_truncate_table(i)
        }
    }

    fn mysql_truncate_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (remaining_input, (_, _, _, _, table, _)) = tuple((
            tag_no_case("truncate"),
            multispace1,
            opt(tag_no_case("table")),
            opt(multispace1),
            |i| self.schema_table(i),
            multispace0,
        ))(i)?;

        let (db, tb) = self.parse_table(table);
        let statement = MysqlTruncateTableStatement {
            db,
            tb,
            unparsed: to_string(remaining_input),
        };

        let ddl = DdlData {
            ddl_type: DdlType::TruncateTable,
            statement: DdlStatement::MysqlTruncateTable(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn pg_truncate_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        // https://www.postgresql.org/docs/16/sql-truncate.html
        let (remaining_input, (_, _, _, _, only, table, _)) = tuple((
            tag_no_case("truncate"),
            multispace1,
            opt(tag_no_case("table")),
            opt(multispace1),
            opt(tuple((tag_no_case("only"), multispace1))),
            |i| self.schema_table(i),
            multispace0,
        ))(i)?;

        let (schema, tb) = self.parse_table(table);
        let statement = PgTruncateTableStatement {
            schema,
            tb,
            is_only: only.is_some(),
            unparsed: to_string(remaining_input),
        };

        let ddl = DdlData {
            ddl_type: DdlType::TruncateTable,
            statement: DdlStatement::PgTruncateTable(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn rename_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (remaining_input, (_, _, _, _, table_to_table_list, _)) = tuple((
            tag_no_case("rename"),
            multispace1,
            tag_no_case("table"),
            multispace1,
            |i| self.schema_table_to_schema_table_list(i),
            multispace0,
        ))(i)?;

        let mut schema_tbs = Vec::new();
        let mut new_schema_tbs = Vec::new();
        for (from, to) in table_to_table_list {
            let from = self.parse_table(from);
            let to = self.parse_table(to);
            schema_tbs.push(from);
            new_schema_tbs.push(to);
        }

        let statement = RenameMultiTableStatement {
            schema_tbs,
            new_schema_tbs,
            unparsed: to_string(remaining_input),
        };

        let ddl = DdlData {
            ddl_type: DdlType::RenameTable,
            statement: DdlStatement::RenameMultiTable(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn create_index<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        match self.db_type {
            DbType::Pg => self.pg_create_index(i),
            DbType::Mssql => self.mssql_create_index(i),
            _ => self.mysql_create_index(i),
        }
    }

    fn mysql_create_index<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        // https://dev.mysql.com/doc/refman/8.4/en/create-index.html
        let (remaining_input, (_, _, index_kind, _, _, index_name, _, index_type, _, _, table, _)) =
            tuple((
                tag_no_case("create"),
                multispace1,
                opt(tuple((
                    alt((
                        tag_no_case("unique"),
                        tag_no_case("fulltext"),
                        tag_no_case("spatial"),
                    )),
                    multispace1,
                ))),
                tag_no_case("index"),
                multispace1,
                |i| self.sql_identifier(i),
                multispace1,
                opt(tuple((
                    tag_no_case("using"),
                    multispace1,
                    alt((tag_no_case("btree"), tag_no_case("hash"))),
                    multispace1,
                ))),
                tag_no_case("on"),
                multispace1,
                |i| self.schema_table(i),
                multispace0,
            ))(i)?;

        let (db, tb) = self.parse_table(table);
        let index_kind_str = if let Some((index_kind, _)) = index_kind {
            Some(to_string(index_kind))
        } else {
            None
        };
        let index_type_str = if let Some((_, _, index_type, _)) = index_type {
            Some(to_string(index_type))
        } else {
            None
        };

        let statement = MysqlCreateIndexStatement {
            db,
            tb,
            index_kind: index_kind_str,
            index_type: index_type_str,
            index_name: self.identifier_to_string(index_name),
            unparsed: to_string(remaining_input),
        };

        let ddl = DdlData {
            ddl_type: DdlType::CreateIndex,
            statement: DdlStatement::MysqlCreateIndex(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn pg_create_index<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        // https://www.postgresql.org/docs/16/sql-createindex.html
        let (remaining_input, (_, _, unique, _, _, concurrently, name, _, _, only, table, _)) =
            tuple((
                tag_no_case("create"),
                multispace1,
                opt(tuple((tag_no_case("unique"), multispace1))),
                tag_no_case("index"),
                multispace1,
                opt(tuple((tag_no_case("concurrently"), multispace1))),
                opt(tuple((
                    opt(if_not_exists),
                    |i| self.sql_identifier_with_keywords(i, &["ON"]),
                    multispace1,
                ))),
                tag_no_case("on"),
                multispace1,
                opt(tuple((tag_no_case("only"), multispace1))),
                |i| self.schema_table(i),
                multispace0,
            ))(i)?;

        let (if_not_exists, index_name) = if let Some(name) = name {
            (name.0.is_some(), Some(self.identifier_to_string(name.1)))
        } else {
            (false, None)
        };

        let (schema, tb) = self.parse_table(table);
        let statement = PgCreateIndexStatement {
            schema,
            tb,
            is_unique: unique.is_some(),
            is_concurrently: concurrently.is_some(),
            is_only: only.is_some(),
            if_not_exists,
            index_name,
            unparsed: to_string(remaining_input),
        };

        let ddl = DdlData {
            ddl_type: DdlType::CreateIndex,
            statement: DdlStatement::PgCreateIndex(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn mssql_create_index<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (
            remaining_input,
            (_, _, unique, clustered, _, _, index_name, _, _, _, (db, schema, tb), _),
        ) = tuple((
            tag_no_case("create"),
            multispace1,
            opt(tuple((tag_no_case("unique"), multispace1))),
            opt(tuple((
                alt((tag_no_case("clustered"), tag_no_case("nonclustered"))),
                multispace1,
            ))),
            tag_no_case("index"),
            multispace1,
            |i| self.sql_identifier(i),
            multispace1,
            tag_no_case("on"),
            multispace1,
            |i| self.mssql_db_schema_table(i),
            multispace0,
        ))(i)?;

        let mut index_kinds = Vec::with_capacity(2);
        if let Some((unique, _)) = unique {
            index_kinds.push(to_string(unique));
        }
        if let Some((clustered, _)) = clustered {
            index_kinds.push(to_string(clustered));
        }
        let statement = MssqlCreateIndexStatement {
            db,
            schema,
            tb,
            index_name: self.identifier_to_string(index_name),
            index_kind: (!index_kinds.is_empty()).then(|| index_kinds.join(" ")),
            unparsed: to_string(remaining_input),
        };
        let ddl = DdlData {
            ddl_type: DdlType::CreateIndex,
            statement: DdlStatement::MssqlCreateIndex(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn drop_index<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        match self.db_type {
            DbType::Pg => self.pg_drop_index(i),
            DbType::Mssql => self.mssql_drop_index(i),
            _ => self.mysql_drop_index(i),
        }
    }

    fn mysql_drop_index<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        // https://dev.mysql.com/doc/refman/8.4/en/drop-index.html
        let (remaining_input, (_, _, _, _, index_name, _, _, _, table, _)) = tuple((
            tag_no_case("drop"),
            multispace1,
            tag_no_case("index"),
            multispace1,
            |i| self.sql_identifier(i),
            multispace1,
            tag_no_case("on"),
            multispace1,
            |i| self.schema_table(i),
            multispace0,
        ))(i)?;

        let (db, tb) = self.parse_table(table);
        let statement = MysqlDropIndexStatement {
            db,
            tb,
            index_name: self.identifier_to_string(index_name),
            unparsed: to_string(remaining_input),
        };

        let ddl = DdlData {
            ddl_type: DdlType::DropIndex,
            statement: DdlStatement::MysqlDropIndex(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn mssql_drop_index<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        let (remaining_input, (_, _, _, _, if_exists, index_name, _, _, _, (db, schema, tb), _)) =
            tuple((
                tag_no_case("drop"),
                multispace1,
                tag_no_case("index"),
                multispace1,
                opt(if_exists),
                |i| self.sql_identifier(i),
                multispace1,
                tag_no_case("on"),
                multispace1,
                |i| self.mssql_db_schema_table(i),
                multispace0,
            ))(i)?;

        let statement = MssqlDropIndexStatement {
            db,
            schema,
            tb,
            index_name: self.identifier_to_string(index_name),
            if_exists: if_exists.is_some(),
            unparsed: to_string(remaining_input),
        };
        let ddl = DdlData {
            ddl_type: DdlType::DropIndex,
            statement: DdlStatement::MssqlDropIndex(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    fn pg_drop_index<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], DdlData> {
        // https://www.postgresql.org/docs/current/sql-dropindex.html
        let (remaining_input, (_, _, _, _, concurrently, if_exists, index_name_list, _)) =
            tuple((
                tag_no_case("drop"),
                multispace1,
                tag_no_case("index"),
                multispace1,
                opt(tuple((tag_no_case("concurrently"), multispace1))),
                opt(if_exists),
                |i| self.sql_identifier_list(i),
                multispace0,
            ))(i)?;

        let mut index_names: Vec<String> = Vec::new();
        for name in index_name_list.iter() {
            index_names.push(self.identifier_to_string(name));
        }

        let statement = PgDropMultiIndexStatement {
            index_names,
            unparsed: to_string(remaining_input),
            if_exists: if_exists.is_some(),
            is_concurrently: concurrently.is_some(),
        };

        let ddl = DdlData {
            ddl_type: DdlType::DropIndex,
            statement: DdlStatement::PgDropMultiIndex(statement),
            ..Default::default()
        };
        Ok((remaining_input, ddl))
    }

    // Parse a reference to a named schema.table, with an optional alias
    fn schema_table<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], SchemaTable> {
        map(
            tuple((
                opt(pair(
                    |i| self.sql_identifier(i),
                    pair(multispace0, tag(".")),
                )),
                multispace0,
                |i| self.sql_identifier(i),
                opt(ws_sep_comma),
            )),
            |tup| {
                let name = tup.2.to_vec();
                let schema = tup.0.map(|(schema, _)| schema.to_vec());
                (schema, name)
            },
        )(i)
    }

    fn schema_table_list<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], Vec<SchemaTable>> {
        many1(|i| self.schema_table(i))(i)
    }

    fn schema_table_to_schema_table<'a>(
        &'a self,
        i: &'a [u8],
    ) -> IResult<&'a [u8], (SchemaTable, SchemaTable)> {
        let (remaining_input, (from_table, _, _, _, to_table, _)) = tuple((
            |i| self.schema_table(i),
            multispace1,
            tag_no_case("to"),
            multispace1,
            |i| self.schema_table(i),
            opt(ws_sep_comma),
        ))(i)?;
        Ok((remaining_input, (from_table, to_table)))
    }

    fn schema_table_to_schema_table_list<'a>(
        &'a self,
        i: &'a [u8],
    ) -> IResult<&'a [u8], Vec<(SchemaTable, SchemaTable)>> {
        many1(|i| self.schema_table_to_schema_table(i))(i)
    }

    fn mssql_db_schema_table<'a>(
        &'a self,
        i: &'a [u8],
    ) -> IResult<&'a [u8], (String, String, String)> {
        let (remaining_input, first) = self.sql_identifier(i)?;
        let first = self.identifier_to_string(first);

        let Ok((remaining_input, _)) = mssql_qualified_separator(remaining_input) else {
            return Ok((remaining_input, (String::new(), String::new(), first)));
        };

        if let Ok((remaining_input, _)) = mssql_qualified_separator(remaining_input) {
            let (remaining_input, table) = self.sql_identifier(remaining_input)?;
            if remaining_input
                .iter()
                .copied()
                .find(|byte| !byte.is_ascii_whitespace())
                == Some(b'.')
            {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    remaining_input,
                    nom::error::ErrorKind::TooLarge,
                )));
            }
            return Ok((
                remaining_input,
                (first, String::new(), self.identifier_to_string(table)),
            ));
        }

        let (remaining_input, second) = self.sql_identifier(remaining_input)?;
        let second = self.identifier_to_string(second);
        let Ok((remaining_input, _)) = mssql_qualified_separator(remaining_input) else {
            return Ok((remaining_input, (String::new(), first, second)));
        };

        let (remaining_input, third) = self.sql_identifier(remaining_input)?;
        if remaining_input
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            == Some(b'.')
        {
            return Err(nom::Err::Failure(nom::error::Error::new(
                remaining_input,
                nom::error::ErrorKind::TooLarge,
            )));
        }
        Ok((
            remaining_input,
            (first, second, self.identifier_to_string(third)),
        ))
    }

    fn sql_identifier<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], &'a [u8]> {
        self.sql_identifier_with_keywords(i, &[])
    }

    fn sql_identifier_with_keywords<'a>(
        &'a self,
        i: &'a [u8],
        forbidden_keywords: &[&str],
    ) -> IResult<&'a [u8], &'a [u8]> {
        if self.db_type == DbType::Mssql {
            return alt((
                preceded(
                    not(peek(|input| self.check_keywords(input, forbidden_keywords))),
                    take_while1(is_mssql_sql_identifier),
                ),
                |i| recognize_doubled_delimited_identifier(i, b'[', b']'),
                |i| recognize_doubled_delimited_identifier(i, b'"', b'"'),
            ))(i);
        }

        if self.db_type == DbType::Pg {
            return alt((
                preceded(
                    not(peek(|input| self.check_keywords(input, forbidden_keywords))),
                    take_while1(is_sql_identifier),
                ),
                // keep tag("\""), input: "Abc", return: "Abc"
                recognize(tuple((
                    tag("\""),
                    take_while1(is_escaped_sql_identifier_2),
                    tag("\""),
                ))),
            ))(i);
        }

        alt((
            preceded(
                not(peek(|input| self.check_keywords(input, forbidden_keywords))),
                take_while1(is_sql_identifier),
            ),
            // remove tag("`"), input: `Abc``, return: Abc
            delimited(tag("`"), take_while1(is_escaped_sql_identifier_1), tag("`")),
        ))(i)
    }

    fn check_keywords<'a>(&'a self, i: &'a [u8], keywords: &[&str]) -> IResult<&'a [u8], &'a [u8]> {
        for &keyword in keywords {
            let keyword_bytes = keyword.as_bytes();
            if let Ok((remaining, matched)) =
                tag_no_case::<_, _, nom::error::Error<&[u8]>>(keyword_bytes)(i)
            {
                if remaining.is_empty() || !is_sql_identifier(remaining[0]) {
                    return Ok((remaining, matched));
                }
            }
        }
        Err(nom::Err::Error(nom::error::Error {
            input: i,
            code: nom::error::ErrorKind::Tag,
        }))
    }

    fn sql_identifier_list<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], Vec<&'a [u8]>> {
        let (remaining_input, identifier_list) =
            many1(tuple((|i| self.sql_identifier(i), opt(ws_sep_comma))))(i)?;
        Ok((
            remaining_input,
            identifier_list
                .into_iter()
                .map(|(identifier, _)| identifier)
                .collect(),
        ))
    }

    // Matches any SQL reserved keyword
    // TODO: remove this function if not used
    #[allow(dead_code)]
    fn _sql_keyword<'a>(&'a self, i: &'a [u8]) -> IResult<&'a [u8], &'a [u8]> {
        alt((
            keyword_a_to_c,
            keyword_c_to_e,
            keyword_e_to_i,
            keyword_i_to_o,
            keyword_o_to_s,
            keyword_s_to_z,
        ))(i)
    }

    fn parse_table(&self, table: (Option<Vec<u8>>, Vec<u8>)) -> (String, String) {
        let schema = if let Some(schema_raw) = &table.0 {
            self.identifier_to_string(schema_raw)
        } else {
            String::new()
        };
        let tb = self.identifier_to_string(&table.1);
        (schema, tb)
    }

    fn identifier_to_string(&self, i: &[u8]) -> String {
        let identifier = to_string(i);
        if self.db_type == DbType::Pg {
            // In PostgreSQL, Identifiers (including column names) that are not double-quoted are folded to lower case.
            // Identifiers created with double quotes retain upper case letters
            let escape_pair = SqlUtil::get_escape_pairs(&self.db_type)[0];
            if SqlUtil::is_escaped(&identifier, &escape_pair) {
                SqlUtil::unescape(&identifier, &escape_pair)
            } else {
                identifier.to_lowercase()
            }
        } else if self.db_type == DbType::Mssql {
            if identifier.starts_with('"') && identifier.ends_with('"') {
                identifier[1..identifier.len() - 1].replace("\"\"", "\"")
            } else {
                SqlUtil::unescape_by_db_type(&identifier, &self.db_type)
            }
        } else {
            identifier
        }
    }
}

#[inline]
fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'_'
}

#[inline]
fn is_mssql_sql_identifier(chr: u8) -> bool {
    is_sql_identifier(chr) || matches!(chr, b'@' | b'#' | b'$')
}

fn mssql_qualified_separator(i: &[u8]) -> IResult<&[u8], ()> {
    map(tuple((multispace0, tag("."), multispace0)), |_| ())(i)
}

fn recognize_doubled_delimited_identifier(i: &[u8], open: u8, close: u8) -> IResult<&[u8], &[u8]> {
    if i.first() != Some(&open) {
        return Err(nom::Err::Error(nom::error::Error::new(
            i,
            nom::error::ErrorKind::Char,
        )));
    }

    let mut index = 1;
    while index < i.len() {
        if i[index] == close {
            if i.get(index + 1) == Some(&close) {
                index += 2;
                continue;
            }
            return Ok((&i[index + 1..], &i[..=index]));
        }
        index += 1;
    }

    Err(nom::Err::Failure(nom::error::Error::new(
        i,
        nom::error::ErrorKind::Escaped,
    )))
}

#[inline]
fn is_escaped_sql_identifier_1(chr: u8) -> bool {
    chr != b'`'
}

#[inline]
fn is_escaped_sql_identifier_2(chr: u8) -> bool {
    chr != b'"'
}

fn if_not_exists(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, _) = tuple((
        tag_no_case("if"),
        multispace1,
        tag_no_case("not"),
        multispace1,
        tag_no_case("exists"),
        multispace1,
    ))(i)?;
    Ok((remaining_input, ()))
}

fn if_exists(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, _) = tuple((
        tag_no_case("if"),
        multispace1,
        tag_no_case("exists"),
        multispace1,
    ))(i)?;
    Ok((remaining_input, ()))
}

fn ws_sep_comma(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(multispace0, tag(","), multispace0)(i)
}

fn to_string(i: &[u8]) -> String {
    String::from_utf8_lossy(i).to_string()
}

#[cfg(test)]
mod test_mysql {

    use super::*;
    use crate::{config::config_enums::DbType, meta::ddl_meta::ddl_parser::DdlParser};

    #[test]
    fn test_create_table_with_schema_mysql() {
        let sqls = [
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
            //  escapes + spaces + if not exists + comments
            "create /*some comments,*/table/*some comments*/ if  not  exists  `aaa` .  `bbb` (id int)  ",
            "create table `special_character_$1#@*_table` (id int)",
            // with database keyword
            "create table query.match (id int)",
        ];

        let expect_sqls = [
            "CREATE TABLE `aaa`.`bbb` (id int)",
            "CREATE TABLE `aaa`.`bbb` (id int)",
            "CREATE TABLE `aaa`.`bbb` (id int)",
            "CREATE TABLE `aaa`.`bbb` (id int)",
            "CREATE TABLE IF NOT EXISTS `aaa`.`bbb` (id int)",
            "CREATE TABLE `aaa`.`bbb` (id int)",
            "CREATE TABLE IF NOT EXISTS `aaa`.`bbb` (id int)",
            "CREATE TABLE `special_character_$1#@*_table` (id int)",
            "CREATE TABLE `query`.`match` (id int)",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_table_with_schema_with_special_characters_mysql() {
        let sqls = [
            "CREATE TABLE IF NOT EXISTS `test_db_*.*`.bbb(id int);",
            "CREATE TABLE IF NOT EXISTS `中文.others*&^%$#@!+_)(&^%#`.`中文!@$#$%^&*&(_+)`(id int);",
        ];

        let expect_sqls = [
            "CREATE TABLE IF NOT EXISTS `test_db_*.*`.`bbb` (id int);",
            "CREATE TABLE IF NOT EXISTS `中文.others*&^%$#@!+_)(&^%#`.`中文!@$#$%^&*&(_+)` (id int);",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_table_without_schema_mysql() {
        let sqls = [
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
        ];

        let expect_sqls = [
            "CREATE TABLE `bbb` (id int)",
            "CREATE TABLE `bbb` (id int)",
            "CREATE TABLE `bbb` (id int)",
            "CREATE TABLE `bbb` (id int)",
            "CREATE TABLE IF NOT EXISTS `bbb` (id int)",
            "CREATE TABLE `bbb` (id int)",
            "CREATE TABLE IF NOT EXISTS `bbb` (id int)",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_drop_table_with_schema_mysql() {
        let sqls = [
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
        ];

        let expect_sqls = [
            "DROP TABLE `aaa`.`bbb`",
            "DROP TABLE `aaa`.`bbb`",
            "DROP TABLE `aaa`.`bbb`",
            "DROP TABLE `aaa`.`bbb`",
            "DROP TABLE IF EXISTS `aaa`.`bbb`",
            "DROP TABLE `aaa`.`bbb`",
            "DROP TABLE IF EXISTS `aaa`.`bbb`",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::DropTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_drop_table_without_schema_mysql() {
        let sqls = [
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
        ];

        let expect_sqls = [
            "DROP TABLE `bbb`",
            "DROP TABLE `bbb`",
            "DROP TABLE `bbb`",
            "DROP TABLE `bbb`",
            "DROP TABLE IF EXISTS `bbb`",
            "DROP TABLE `bbb`",
            "DROP TABLE IF EXISTS `bbb`",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::DropTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_table_with_schema_mysql() {
        let sqls = [
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
        ];

        let expect_sqls = [
            "ALTER TABLE `aaa`.`bbb` add column value int",
            // escapes
            "ALTER TABLE `aaa`.`bbb` add column value int",
            // spaces
            "ALTER TABLE `aaa`.`bbb` add column value int",
            // spaces + escapes
            "ALTER TABLE `aaa`.`bbb` add column value int",
            // if exists
            "ALTER TABLE `aaa`.`bbb` add column value int",
            // comments
            "ALTER TABLE `aaa`.`bbb` add column value int",
            //  escapes + spaces + comments
            "ALTER TABLE `aaa`.`bbb` add column value int",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::AlterTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_table_without_schema_mysql() {
        let sqls = [
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
        ];

        let expect_sqls = [
            // schema.table
            "ALTER TABLE `bbb` add column value int",
            // escapes
            "ALTER TABLE `bbb` add column value int",
            // spaces
            "ALTER TABLE `bbb` add column value int",
            // spaces + escapes
            "ALTER TABLE `bbb` add column value int",
            // comments
            "ALTER TABLE `bbb` add column value int",
            // escapes + spaces + comments
            "ALTER TABLE `bbb` add column value int",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::AlterTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_rename_table_mysql() {
        let sqls = [
            "ALTER TABLE tb_2 RENAME  tb_3",
            "alter table tb_2 rename as tb_3",
            "alter table tb_2 rename to tb_3",
            "ALTER TABLE `db_1`.tb_2 RENAME  `db_2`.tb_3",
            "alter table `db_1`.tb_2 rename as `db_2`.tb_3",
            "alter table `db_1`.tb_2 rename to `db_2`.tb_3",
        ];

        let expect_sqls = [
            "ALTER TABLE `tb_2` RENAME TO `tb_3`",
            "ALTER TABLE `tb_2` RENAME TO `tb_3`",
            "ALTER TABLE `tb_2` RENAME TO `tb_3`",
            "ALTER TABLE `db_1`.`tb_2` RENAME TO `db_2`.`tb_3`",
            "ALTER TABLE `db_1`.`tb_2` RENAME TO `db_2`.`tb_3`",
            "ALTER TABLE `db_1`.`tb_2` RENAME TO `db_2`.`tb_3`",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::AlterTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_database_mysql() {
        let sqls = [
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
        ];

        let expect_sqls = [
            "CREATE DATABASE `aaa`",
            // escapes
            "CREATE DATABASE `aaa`",
            // spaces
            "CREATE DATABASE `aaa`",
            // spaces + escapes
            "CREATE DATABASE `aaa`",
            // if exists
            "CREATE DATABASE IF NOT EXISTS `aaa`",
            // comments
            "CREATE DATABASE `aaa`",
            //  escapes + spaces + if exists + comments
            "CREATE DATABASE IF NOT EXISTS `aaa`",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateDatabase);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_database_with_special_characters_mysql() {
        let sqls = [
            "CREATE DATABASE IF NOT EXISTS `test_db_*.*`;",
            "CREATE DATABASE IF NOT EXISTS `中文.others*&^%$#@!+_)(&^%#`;",
        ];

        let expect_sqls = [
            "CREATE DATABASE IF NOT EXISTS `test_db_*.*` ;",
            "CREATE DATABASE IF NOT EXISTS `中文.others*&^%$#@!+_)(&^%#` ;",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateDatabase);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_drop_database_mysql() {
        let sqls = [
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
        ];

        let expect_sqls = [
            "DROP DATABASE `aaa`",
            // escapes
            "DROP DATABASE `aaa`",
            // spaces
            "DROP DATABASE `aaa`",
            // spaces + escapes
            "DROP DATABASE `aaa`",
            // if exists
            "DROP DATABASE IF EXISTS `aaa`",
            // comments
            "DROP DATABASE `aaa`",
            //  escapes + spaces + if exists + comments
            "DROP DATABASE IF EXISTS `aaa`",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::DropDatabase);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_database_mysql() {
        let sqls = [
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
        ];

        let expect_sqls = [
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
            // escapes
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
            // spaces
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
            // spaces + escapes
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
            // comments
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
            // escapes + spaces + comments
            "ALTER DATABASE `aaa` CHARACTER SET utf8",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::AlterDatabase);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_truncate_table_with_schema_mysql() {
        let sqls = [
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
        ];

        let expect_sqls = [
            // schema.table
            "TRUNCATE TABLE `aaa`.`bbb`",
            // escapes
            "TRUNCATE TABLE `aaa`.`bbb`",
            // spaces
            "TRUNCATE TABLE `aaa`.`bbb`",
            // spaces + escapes
            "TRUNCATE TABLE `aaa`.`bbb`",
            // comments
            "TRUNCATE TABLE `aaa`.`bbb`",
            //  escapes + spaces + comments
            "TRUNCATE TABLE `aaa`.`bbb`",
            // without keyword `table`
            "TRUNCATE TABLE `aaa`.`bbb`",
            "TRUNCATE TABLE `aaa`.`bbb`",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::TruncateTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_truncate_table_without_schema_mysql() {
        let sqls = [
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
        ];

        let expect_sqls = [
            // schema.table
            "TRUNCATE TABLE `bbb`",
            // escapes
            "TRUNCATE TABLE `bbb`",
            // spaces
            "TRUNCATE TABLE `bbb`",
            // spaces + escapes
            "TRUNCATE TABLE `bbb`",
            // comments
            "TRUNCATE TABLE `bbb`",
            //  escapes + spaces + comments
            "TRUNCATE TABLE `bbb`",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::TruncateTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_rename_table_mysql() {
        let sqls = [
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
        ];

        let expect_sqls = [
            // schema.table
            "RENAME TABLE `aaa`.`bbb` TO `aaa`.`ccc`",
            // escapes
            "RENAME TABLE `aaa`.`bbb` TO `aaa`.`ccc`",
            // spaces
            "RENAME TABLE `aaa`.`bbb` TO `aaa`.`ccc`",
            // spaces + escapes
            "RENAME TABLE `aaa`.`bbb` TO `aaa`.`ccc`",
            // comments
            "RENAME TABLE `aaa`.`bbb` TO `aaa`.`ccc`",
            //  escapes + spaces + comments
            "RENAME TABLE `aaa`.`bbb` TO `aaa`.`ccc`",
            // multiple tables
            "RENAME TABLE `aaa`.`bbb` TO `aaa`.`ccc`, `bbb`.`ddd` TO `eee`.`fff`, `中文`.`中文😀` TO `中文😀`.`中文`",
            "RENAME TABLE `bbb` TO `ccc`, `ddd` TO `fff`, `中文😀` TO `中文`",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::RenameTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_index_mysql() {
        let sqls = [
            "create index idx2 on t1 ((col1 + col2), (col1 - col2), col1);",
            "create unique index `idx2` using  btree  on `d1`.`t1`((col1 + col2), (col1 - col2), col1);",
            "create index query on db1.match(a,b);",
        ];

        let expect_sqls = [
            "CREATE INDEX `idx2` ON `t1` ((col1 + col2), (col1 - col2), col1);",
            "CREATE UNIQUE INDEX `idx2` USING BTREE ON `d1`.`t1` ((col1 + col2), (col1 - col2), col1);",
            "CREATE INDEX `query` ON `db1`.`match` (a,b);",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateIndex);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_drop_index_mysql() {
        let sqls = [
            "drop index index1 on t1 algorithm=default;",
            // escapes
            "drop index `index1` on `d1`.`t1` algorithm=default;",
        ];

        let expect_sqls = [
            "DROP INDEX `index1` ON `t1` algorithm=default;",
            "DROP INDEX `index1` ON `d1`.`t1` algorithm=default;",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::DropIndex);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }
}

#[cfg(test)]
mod test_pg {
    use crate::{
        config::config_enums::DbType,
        meta::ddl_meta::{ddl_parser::DdlParser, ddl_type::DdlType},
    };

    #[test]
    fn test_create_table_multi_lines_pg() {
        let sqls = [r#"CREATE TABLE -- some comments
            IF NOT EXISTS 
            db_1.tb_1 
            (id int,
            value int);"#];

        let expect_sqls =
            ["CREATE TABLE IF NOT EXISTS \"db_1\".\"tb_1\" (id int,\n            value int);"];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_table_with_schema_with_upper_case_pg() {
        let sqls = [
            r#"CREATE TABLE IF NOT EXISTS Test_DB.Test_TB(id int, "Value" int);"#,
            r#"CREATE TABLE IF NOT EXISTS "Test_DB".Test_TB(id int, "Value" int);"#,
            r#"CREATE TABLE IF NOT EXISTS "Test_DB"."Test_TB"(id int, "Value" int);"#,
        ];

        let expect_sqls = [
            r#"CREATE TABLE IF NOT EXISTS "test_db"."test_tb" (id int, "Value" int);"#,
            r#"CREATE TABLE IF NOT EXISTS "Test_DB"."test_tb" (id int, "Value" int);"#,
            r#"CREATE TABLE IF NOT EXISTS "Test_DB"."Test_TB" (id int, "Value" int);"#,
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_table_with_schema_with_special_characters_pg() {
        let sqls = [
            r#"CREATE TABLE IF NOT EXISTS "test_db_*.*".bbb(id int);"#,
            r#"CREATE TABLE IF NOT EXISTS "中文.others*&^%$#@!+_)(&^%#"."中文!@$#$%^&*&(_+)"(id int);"#,
            r#"CREATE TABLE IF NOT EXISTS query.match(id int);"#,
        ];

        let expect_sqls = [
            r#"CREATE TABLE IF NOT EXISTS "test_db_*.*"."bbb" (id int);"#,
            r#"CREATE TABLE IF NOT EXISTS "中文.others*&^%$#@!+_)(&^%#"."中文!@$#$%^&*&(_+)" (id int);"#,
            r#"CREATE TABLE IF NOT EXISTS "query"."match" (id int);"#,
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_table_with_temporary_pg() {
        let sqls = [
            r#"create UNLOGGED table tb_1(ts TIMESTAMP);"#,
            r#"create TEMPORARY table tb_2(ts TIMESTAMP);"#,
            r#"create temp table tb_3(ts TIMESTAMP);"#,
            r#"create GLOBAL TEMPORARY table tb_4(ts TIMESTAMP) ON COMMIT DELETE ROWS;"#,
            r#"create local temp table tb_5(ts TIMESTAMP);"#,
        ];

        let expect_sqls = [
            r#"CREATE UNLOGGED TABLE "tb_1" (ts TIMESTAMP);"#,
            r#"CREATE TEMPORARY TABLE "tb_2" (ts TIMESTAMP);"#,
            r#"CREATE temp TABLE "tb_3" (ts TIMESTAMP);"#,
            r#"CREATE GLOBAL TEMPORARY TABLE "tb_4" (ts TIMESTAMP) ON COMMIT DELETE ROWS;"#,
            r#"CREATE local temp TABLE "tb_5" (ts TIMESTAMP);"#,
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_table_with_schema_pg() {
        let sqls = [
            // escapes + spaces + comments
            r#"alter /*some comments,*/table/*some comments*/   "aaa" .  "bbb"   add column value int"#,
            // escapes + spaces + if exists + only + comments
            r#"alter /*some comments,*/table
            if exists
            only
            -- some comments
            "aaa" .  "bbb"  
            add column 
            value int"#,
        ];

        let expect_sqls = [
            // escapes + spaces + comments
            r#"ALTER TABLE "aaa"."bbb" add column value int"#,
            // escapes + spaces + if exists + comments
            "ALTER TABLE IF EXISTS ONLY \"aaa\".\"bbb\" add column \n            value int",
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::AlterTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_table_without_schema_pg() {
        let sqls = [
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
        ];

        let expect_sqls = [
            // schema.table
            "ALTER TABLE `bbb` add column value int",
            // escapes
            "ALTER TABLE `bbb` add column value int",
            // spaces
            "ALTER TABLE `bbb` add column value int",
            // spaces + escapes
            "ALTER TABLE `bbb` add column value int",
            // comments
            "ALTER TABLE `bbb` add column value int",
            // escapes + spaces + comments
            "ALTER TABLE `bbb` add column value int",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::AlterTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_rename_table_pg() {
        let sqls = [
            "ALTER TABLE tb_1 RENAME TO tb_2",
            "alter table tb_1 rename to tb_2",
            r#"ALTER TABLE IF EXISTS ONLY "schema_1".tb_1 RENAME TO tb_2"#,
            r#"alter table "schema_1".tb_1 rename to tb_2"#,
            r#"ALTER TABLE IF EXISTS ONLY "schema_1".tb_1 SET SCHEMA tb_2"#,
            r#"alter table "schema_1".tb_1 set schema tb_2"#,
        ];

        let expect_sqls = [
            r#"ALTER TABLE "tb_1" RENAME TO "tb_2""#,
            r#"ALTER TABLE "tb_1" RENAME TO "tb_2""#,
            r#"ALTER TABLE IF EXISTS ONLY "schema_1"."tb_1" RENAME TO "tb_2""#,
            r#"ALTER TABLE "schema_1"."tb_1" RENAME TO "tb_2""#,
            r#"ALTER TABLE IF EXISTS ONLY "schema_1"."tb_1" SET SCHEMA "tb_2""#,
            r#"ALTER TABLE "schema_1"."tb_1" SET SCHEMA "tb_2""#,
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::AlterTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_schema_pg() {
        let sqls = [
            "create schema aaa",
            // escapes
            "create schema \"aaa\"",
            // spaces
            "  create   schema   aaa",
            // spaces + escapes
            "  create   schema   \"aaa\"  ",
            // if exists
            "create schema if  not  exists \"aaa\"",
            // comments
            "create /*some comments,*/schema/*some comments*/ \"aaa\"",
            //  escapes + spaces + if exists + comments
            "create /*some comments,*/schema/*some comments*/ if  not  exists    \"aaa\"  ",
            "create schema authorization aaa",
            "create schema if not exists match authorization hehe",
        ];

        let expect_sqls = [
            r#"CREATE SCHEMA "aaa""#,
            // escapes
            r#"CREATE SCHEMA "aaa""#,
            // spaces
            r#"CREATE SCHEMA "aaa""#,
            // spaces + escapes
            r#"CREATE SCHEMA "aaa""#,
            // if exists
            r#"CREATE SCHEMA IF NOT EXISTS "aaa""#,
            // comments
            r#"CREATE SCHEMA "aaa""#,
            //  escapes + spaces + if exists + comments
            r#"CREATE SCHEMA IF NOT EXISTS "aaa""#,
            r#"CREATE SCHEMA AUTHORIZATION "aaa""#,
            r#"CREATE SCHEMA IF NOT EXISTS "match" authorization hehe"#,
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateSchema);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_schema_with_special_characters_pg() {
        let sqls = [
            "CREATE SCHEMA IF NOT EXISTS \"test_db_*.*\";",
            "CREATE SCHEMA IF NOT EXISTS \"中文.others*&^%$#@!+_)(&^%#\";",
        ];

        let expect_sqls = [
            "CREATE SCHEMA IF NOT EXISTS \"test_db_*.*\" ;",
            "CREATE SCHEMA IF NOT EXISTS \"中文.others*&^%$#@!+_)(&^%#\" ;",
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateSchema);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_drop_schema_pg() {
        let sqls = [
            "drop schema aaa",
            // escapes
            "drop schema \"aaa\"",
            // spaces
            "  drop   schema   aaa",
            // spaces + escapes
            "  drop   schema   \"aaa\"  ",
            // if exists
            "drop schema if  exists \"aaa\"",
            // comments
            "drop /*some comments,*/schema/*some comments*/ \"aaa\"",
            //  escapes + spaces + if exists + comments
            "drop /*some comments,*/schema/*some comments*/ if  exists    \"aaa\"  ",
        ];

        let expect_sqls = [
            r#"DROP SCHEMA "aaa""#,
            // escapes
            r#"DROP SCHEMA "aaa""#,
            // spaces
            r#"DROP SCHEMA "aaa""#,
            // spaces + escapes
            r#"DROP SCHEMA "aaa""#,
            // if exists
            r#"DROP SCHEMA IF EXISTS "aaa""#,
            // comments
            r#"DROP SCHEMA "aaa""#,
            //  escapes + spaces + if exists + comments
            r#"DROP SCHEMA IF EXISTS "aaa""#,
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::DropSchema);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_alter_schema_pg() {
        let sqls = [
            "alter schema aaa rename to bbb",
            // escapes
            "alter schema \"aaa\" rename to bbb",
            // spaces
            "  alter   schema   aaa rename to bbb",
            // spaces + escapes
            "  alter   schema   \"aaa\"   rename to bbb",
            // comments
            "alter /*some comments,*/schema/*some comments*/ \"aaa\" rename to bbb",
            //  escapes + spaces + comments
            "alter /*some comments,*/schema/*some comments*/    \"aaa\"   rename to bbb",
            "alter schema aaa owner to bbb",
        ];

        let expect_sqls = [
            r#"ALTER SCHEMA "aaa" RENAME TO "bbb""#,
            // escapes
            r#"ALTER SCHEMA "aaa" RENAME TO "bbb""#,
            // spaces
            r#"ALTER SCHEMA "aaa" RENAME TO "bbb""#,
            // spaces + escapes
            r#"ALTER SCHEMA "aaa" RENAME TO "bbb""#,
            // comments
            r#"ALTER SCHEMA "aaa" RENAME TO "bbb""#,
            //  escapes + spaces + comments
            r#"ALTER SCHEMA "aaa" RENAME TO "bbb""#,
            r#"ALTER SCHEMA "aaa" owner to bbb"#,
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::AlterSchema);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_truncate_table_pg() {
        let sqls = [
            // schema.table
            "truncate table aaa.bbb",
            // escapes + spaces + comments
            r#"truncate /*some comments,*/table/*some comments*/   "aaa" .  "bbb"  "#,
            // without keyword `table`
            r#"truncate /*some comments,*/   "aaa" .  "bbb"  "#,
            // with keyword `only`
            r#"truncate /*some comments,*/table/*some comments*/  ONLY "aaa"."bbb""#,
            r#"truncate /*some comments,*/  ONLY "aaa"."bbb""#,
        ];

        let expect_sqls = [
            r#"TRUNCATE TABLE "aaa"."bbb""#,
            r#"TRUNCATE TABLE "aaa"."bbb""#,
            r#"TRUNCATE TABLE "aaa"."bbb""#,
            r#"TRUNCATE TABLE ONLY "aaa"."bbb""#,
            r#"TRUNCATE TABLE ONLY "aaa"."bbb""#,
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::TruncateTable);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_create_index_pg() {
        let sqls = [
            r#"create index on "tb_1"(id);"#,
            r#"create unique index
            concurrently -- some comments
            "idx3" on only "tb_1"(a);"#,
            r#"create
            unique
            index
            concurrently -- some comments
            if not
            exists
            "idx3"
            on
            only
            "tb_1"(a);"#,
        ];

        let expect_sqls = [
            "CREATE INDEX ON \"tb_1\" (id);",
            "CREATE UNIQUE INDEX CONCURRENTLY \"idx3\" ON ONLY \"tb_1\" (a);",
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS \"idx3\" ON ONLY \"tb_1\" (a);",
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::CreateIndex);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }

    #[test]
    fn test_drop_index_pg() {
        let sqls = [
            "drop index tb_1_id_idx",
            r#"drop index if exists tb_1_id_idx,tb_1_id_idx1 RESTRICT;"#,
            r#"drop index CONCURRENTLY if exists tb_1_id_idx3 RESTRICT;"#,
        ];

        let expect_sqls = [
            r#"DROP INDEX "tb_1_id_idx""#,
            r#"DROP INDEX IF EXISTS "tb_1_id_idx", "tb_1_id_idx1" RESTRICT;"#,
            r#"DROP INDEX CONCURRENTLY IF EXISTS "tb_1_id_idx3" RESTRICT;"#,
        ];

        let parser = DdlParser::new(DbType::Pg);
        for i in 0..sqls.len() {
            let r = parser.parse(sqls[i]).unwrap().unwrap();
            assert_eq!(r.ddl_type, DdlType::DropIndex);
            assert_eq!(r.to_sql(), expect_sqls[i]);
        }
    }
}

#[cfg(test)]
mod test_mssql {
    use super::*;

    #[test]
    fn test_supported_ddl_mssql() {
        let cases = [
            (
                "CREATE DATABASE [db.with.dot]",
                DdlType::CreateDatabase,
                "",
                "",
                ("db.with.dot", "", ""),
                "CREATE DATABASE [db.with.dot]",
            ),
            (
                "DROP DATABASE IF EXISTS [escaped]]db]",
                DdlType::DropDatabase,
                "",
                "",
                ("escaped]db", "", ""),
                "DROP DATABASE IF EXISTS [escaped]]db]",
            ),
            (
                "CREATE SCHEMA [schema.with.dot]",
                DdlType::CreateSchema,
                "app",
                "dbo",
                ("app", "schema.with.dot", ""),
                "CREATE SCHEMA [schema.with.dot]",
            ),
            (
                "DROP SCHEMA IF EXISTS [escaped]]schema]",
                DdlType::DropSchema,
                "app",
                "dbo",
                ("app", "escaped]schema", ""),
                "DROP SCHEMA IF EXISTS [escaped]]schema]",
            ),
            (
                "CREATE TABLE [db.with.dot].[schema]]name].[table with space] (id int)",
                DdlType::CreateTable,
                "default_db",
                "dbo",
                ("db.with.dot", "schema]name", "table with space"),
                "CREATE TABLE [db.with.dot].[schema]]name].[table with space] (id int)",
            ),
            (
                "DROP TABLE IF EXISTS [app].[dbo].[orders]",
                DdlType::DropTable,
                "",
                "",
                ("app", "dbo", "orders"),
                "DROP TABLE IF EXISTS [app].[dbo].[orders]",
            ),
            (
                "CREATE UNIQUE NONCLUSTERED INDEX [idx.with.dot] ON [app].[audit].[orders] (id)",
                DdlType::CreateIndex,
                "",
                "",
                ("app", "audit", "orders"),
                "CREATE UNIQUE NONCLUSTERED INDEX [idx.with.dot] ON [app].[audit].[orders] (id)",
            ),
            (
                "DROP INDEX IF EXISTS [idx.with.dot] ON [app].[audit].[orders]",
                DdlType::DropIndex,
                "",
                "",
                ("app", "audit", "orders"),
                "DROP INDEX IF EXISTS [idx.with.dot] ON [app].[audit].[orders]",
            ),
            (
                "CREATE TABLE [orders] (id int)",
                DdlType::CreateTable,
                "app",
                "dbo",
                ("app", "dbo", "orders"),
                "CREATE TABLE [orders] (id int)",
            ),
            (
                "CREATE TABLE [app]..[orders] (id int)",
                DdlType::CreateTable,
                "",
                "",
                ("app", "", "orders"),
                "CREATE TABLE [app]..[orders] (id int)",
            ),
        ];

        let parser = DdlParser::new(DbType::Mssql);
        for (sql, ddl_type, default_db, default_schema, expected_name, expected_sql) in cases {
            let mut ddl = parser.parse(sql).unwrap().unwrap();
            assert!(ddl.default_db.is_empty(), "sql: {sql}");
            assert!(ddl.default_schema.is_empty(), "sql: {sql}");
            ddl.default_db = default_db.to_string();
            ddl.default_schema = default_schema.to_string();
            ddl.query = sql.to_string();

            assert_eq!(ddl.ddl_type, ddl_type, "sql: {sql}");
            match ddl.ddl_type {
                DdlType::CreateDatabase => {
                    assert!(matches!(
                        &ddl.statement,
                        DdlStatement::MssqlCreateDatabase(_)
                    ))
                }
                DdlType::DropDatabase => {
                    assert!(matches!(&ddl.statement, DdlStatement::MssqlDropDatabase(_)))
                }
                DdlType::CreateSchema => {
                    assert!(matches!(&ddl.statement, DdlStatement::MssqlCreateSchema(_)))
                }
                DdlType::DropSchema => {
                    assert!(matches!(&ddl.statement, DdlStatement::MssqlDropSchema(_)))
                }
                DdlType::CreateTable => {
                    assert!(matches!(&ddl.statement, DdlStatement::MssqlCreateTable(_)))
                }
                DdlType::DropTable => {
                    assert!(matches!(&ddl.statement, DdlStatement::MssqlDropTable(_)))
                }
                DdlType::CreateIndex => {
                    assert!(matches!(&ddl.statement, DdlStatement::MssqlCreateIndex(_)))
                }
                DdlType::DropIndex => {
                    assert!(matches!(&ddl.statement, DdlStatement::MssqlDropIndex(_)))
                }
                _ => {}
            }
            assert_eq!(
                ddl.get_db_schema_tb(),
                (
                    expected_name.0.to_string(),
                    expected_name.1.to_string(),
                    expected_name.2.to_string(),
                ),
                "sql: {sql}"
            );
            assert_eq!(ddl.to_sql(), expected_sql, "sql: {sql}");
        }
    }

    #[test]
    fn test_unimplemented_ddl_mssql() {
        for sql in [
            "CREATE TABLE [server].[app].[dbo].[orders] (id int)",
            "ALTER DATABASE [app] SET READ_ONLY",
            "ALTER SCHEMA [audit] TRANSFER [dbo].[orders]",
            "ALTER TABLE [app].[dbo].[orders] ADD value int",
            "TRUNCATE TABLE [app].[dbo].[orders]",
            "RENAME TABLE [dbo].[orders] TO [dbo].[orders_2]",
        ] {
            assert!(DdlParser::new(DbType::Mssql).parse(sql).is_err(), "{sql}");
        }
    }
}

#[cfg(test)]
mod test_common {
    use crate::{config::config_enums::DbType, meta::ddl_meta::ddl_parser::DdlParser};

    #[test]
    fn test_ddl_simple_judgment() {
        let sqls = [
            "INSERT INTO kubeblocks.kb_health_check VALUES(1, UNIX_TIMESTAMP()) ON DUPLICATE KEY UPDATE check_ts = UNIX_TIMESTAMP()",
            "REPLACE INTO kubeblocks.kb_health_check VALUES(1, UNIX_TIMESTAMP())",
            "UPDATE kubeblocks.kb_health_check SET check_ts = UNIX_TIMESTAMP() WHERE id = 1",
            "DELETE FROM kubeblocks.kb_health_check WHERE id = 1",
        ];

        let parser = DdlParser::new(DbType::Mysql);
        for sql in sqls {
            assert!(!DdlParser::ddl_simple_judgment(sql));
            assert!(parser.parse(sql).unwrap().is_none());
        }
    }
}

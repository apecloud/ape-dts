use anyhow::bail;

use crate::config::config_enums::DbType;
use crate::error::Error;
use crate::meta::ddl_meta::ddl_parser::DdlParser;
use crate::meta::ddl_meta::ddl_statement::DdlStatement;
use crate::meta::struct_meta::structure::column::ColumnDefault;
use crate::rdb_filter::RdbFilter;

use crate::meta::struct_meta::structure::{
    column::Column,
    comment::Comment,
    constraint::{Constraint, ConstraintType},
    index::{Index, IndexKind},
    sequence::Sequence,
    sequence_owner::SequenceOwner,
    structure_type::StructureType,
    table::Table,
};

#[derive(Debug, Clone)]
pub struct PgCreateTableStatement {
    pub table: Table,
    pub table_comments: Vec<Comment>,
    pub column_comments: Vec<Comment>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Index>,
    pub sequences: Vec<Sequence>,
    pub sequence_owners: Vec<SequenceOwner>,
}

impl PgCreateTableStatement {
    pub fn route(&mut self, dst_schema: &str, dst_tb: &str) {
        self.table.schema_name = dst_schema.to_string();
        self.table.table_name = dst_tb.to_string();
        for comment in self.table_comments.iter_mut() {
            comment.schema_name = dst_schema.to_string();
            comment.table_name = dst_tb.to_string();
        }

        for comment in self.column_comments.iter_mut() {
            comment.schema_name = dst_schema.to_string();
            comment.table_name = dst_tb.to_string();
        }

        for constraint in self.constraints.iter_mut() {
            constraint.schema_name = dst_schema.to_string();
            constraint.table_name = dst_tb.to_string();
        }

        for index in self.indexes.iter_mut() {
            index.schema_name = dst_schema.to_string();
            index.table_name = dst_tb.to_string();
        }

        for sequence in self.sequences.iter_mut() {
            sequence.schema_name = dst_schema.to_string();
        }

        for owner in self.sequence_owners.iter_mut() {
            owner.schema_name = dst_schema.to_string();
            owner.table_name = dst_tb.to_string();
        }
    }

    pub fn to_sqls(&mut self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        let mut sqls = Vec::new();

        if !filter.filter_structure(&StructureType::Table) {
            for i in self.sequences.iter() {
                let key = format!("sequence.{}.{}", i.schema_name, i.sequence_name);
                sqls.push((key, Self::sequence_to_sql(i)));
            }

            let key = format!("table.{}.{}", self.table.schema_name, self.table.table_name);
            sqls.push((key, Self::table_to_sql(&mut self.table)));

            for i in self.sequence_owners.iter() {
                let key = format!(
                    "sequence_owner.{}.{}.{}",
                    i.schema_name, i.table_name, i.sequence_name
                );
                sqls.push((key, Self::sequence_owner_to_sql(i)));
            }

            for i in self.column_comments.iter() {
                let key = format!(
                    "column_comment.{}.{}.{}",
                    i.schema_name, i.table_name, i.column_name
                );
                sqls.push((key, Self::comment_to_sql(i)));
            }

            for i in self.table_comments.iter() {
                let key = format!("table_comment.{}.{}", i.schema_name, i.table_name);
                sqls.push((key, Self::comment_to_sql(i)));
            }
        }

        for i in self.constraints.iter() {
            match i.constraint_type {
                ConstraintType::Primary | ConstraintType::Unique => {
                    if filter.filter_structure(&StructureType::Table) {
                        continue;
                    }
                }
                _ => {
                    if filter.filter_structure(&StructureType::Constraint) {
                        continue;
                    }
                }
            }

            let key = format!(
                "constraint.{}.{}.{}",
                i.schema_name, i.table_name, i.constraint_name
            );
            sqls.push((key, Self::constraint_to_sql(i)));
        }

        for i in self.indexes.iter() {
            match i.index_kind {
                IndexKind::Unique => {
                    if filter.filter_structure(&StructureType::Table) {
                        continue;
                    }
                }
                _ => {
                    if filter.filter_structure(&StructureType::Index) {
                        continue;
                    }
                }
            }

            let key = format!("index.{}.{}.{}", i.schema_name, i.table_name, i.index_name);
            sqls.push((key, Self::index_to_sql(i)?));
        }

        Ok(sqls)
    }

    fn table_to_sql(table: &mut Table) -> String {
        let columns_sql = Self::columns_to_sql(&mut table.columns);
        format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"."{}" ({})"#,
            table.schema_name, table.table_name, columns_sql
        )
    }

    fn columns_to_sql(columns: &mut [Column]) -> String {
        let mut sql = String::new();

        columns.sort_by(|a, b| a.ordinal_position.cmp(&b.ordinal_position));
        for column in columns.iter() {
            sql.push_str(format!(r#""{}" {} "#, column.column_name, column.column_type).as_str());
            if !column.is_nullable {
                sql.push_str("NOT NULL ");
            }
            match &column.column_default {
                Some(ColumnDefault::Expression(v)) | Some(ColumnDefault::Literal(v)) => {
                    sql.push_str(format!("DEFAULT {} ", v).as_str())
                }
                None => {}
            }
            if let Some(x) = &column.generated {
                if x == "ALWAYS" {
                    sql.push_str("GENERATED ALWAYS AS IDENTITY ")
                } else {
                    sql.push_str("GENERATED BY DEFAULT AS IDENTITY ")
                }
            }
            sql.push(',');
        }

        if sql.ends_with(',') {
            sql = sql[0..sql.len() - 1].to_string();
        }

        sql
    }

    fn index_to_sql(index: &Index) -> anyhow::Result<String> {
        let parser = DdlParser::new(DbType::Pg);
        if let Ok(Some(mut ddl_data)) = parser.parse(&index.definition) {
            if let DdlStatement::PgCreateIndex(s) = &mut ddl_data.statement {
                s.schema = index.schema_name.clone();
                s.tb = index.table_name.clone();
                s.if_not_exists = true;
            }
            Ok(ddl_data.to_sql())
        } else {
            bail! {Error::Unexpected( format!(
                "failed to parse index, schema: {}, tb: {}, definition: {}",
                &index.schema_name, &index.table_name, index.definition
            ) )}
        }
    }

    fn comment_to_sql(comment: &Comment) -> String {
        if comment.column_name.is_empty() {
            format!(
                r#"COMMENT ON TABLE "{}"."{}" is '{}'"#,
                comment.schema_name, comment.table_name, comment.comment
            )
        } else {
            format!(
                r#"COMMENT ON COLUMN "{}"."{}"."{}" IS '{}'"#,
                comment.schema_name, comment.table_name, comment.column_name, comment.comment
            )
        }
    }

    fn sequence_to_sql(sequence: &Sequence) -> String {
        let cycle_str = if sequence.cycle_option.to_lowercase() == "yes" {
            "CYCLE"
        } else {
            "NO CYCLE"
        };

        format!(
            r#"CREATE SEQUENCE IF NOT EXISTS "{}"."{}" AS {} START {} INCREMENT by {} MINVALUE {} MAXVALUE {} {}"#,
            sequence.schema_name,
            sequence.sequence_name,
            sequence.data_type,
            sequence.start_value,
            sequence.increment,
            sequence.minimum_value,
            sequence.maximum_value,
            cycle_str
        )
    }

    fn sequence_owner_to_sql(sequence_owner: &SequenceOwner) -> String {
        format!(
            r#"ALTER SEQUENCE "{}"."{}" OWNED BY "{}"."{}"."{}""#,
            sequence_owner.schema_name,
            sequence_owner.sequence_name,
            sequence_owner.schema_name,
            sequence_owner.table_name,
            sequence_owner.column_name
        )
    }

    fn constraint_to_sql(constraint: &Constraint) -> String {
        format!(
            r#"ALTER TABLE "{}"."{}" ADD CONSTRAINT "{}" {}"#,
            constraint.schema_name,
            constraint.table_name,
            constraint.constraint_name,
            constraint.definition
        )
    }
}

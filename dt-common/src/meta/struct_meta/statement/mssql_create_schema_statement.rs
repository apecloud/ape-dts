use super::mssql_comment_statement::MssqlComment;
use crate::{
    config::config_enums::DbType, meta::struct_meta::structure::structure_type::StructureType,
    rdb_filter::RdbFilter, utils::sql_util::SqlUtil,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlSequence {
    pub sequence_name: String,
    pub data_type: String,
    pub start_value: String,
    pub increment: String,
    pub minimum_value: String,
    pub maximum_value: String,
    pub is_cycling: bool,
    pub is_cached: bool,
    pub cache_size: Option<u64>,
    pub comments: Vec<MssqlComment>,
}

#[derive(Debug, Clone)]
pub struct MssqlCreateSchemaStatement {
    pub database_name: String,
    pub schema_name: String,
    pub sequences: Vec<MssqlSequence>,
    pub comments: Vec<MssqlComment>,
}

impl MssqlCreateSchemaStatement {
    pub fn route_database(&mut self, dst_db: &str) {
        self.database_name = dst_db.to_string();
    }

    pub fn to_sqls(&self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        let mut sqls = Vec::new();
        if !filter.filter_structure(&StructureType::Database) {
            sqls.push((
                format!("schema.{}.{}", self.database_name, self.schema_name),
                self.create_schema_sql(),
            ));
        }

        for sequence in &self.sequences {
            if !filter.filter_structure(&StructureType::Sequence) {
                sqls.push((
                    format!(
                        "sequence.{}.{}.{}",
                        self.database_name, self.schema_name, sequence.sequence_name
                    ),
                    self.create_sequence_sql(sequence),
                ));
            }
            for comment in &sequence.comments {
                if let Some(sql) = comment.to_sql(
                    &self.database_name,
                    &self.schema_name,
                    &sequence.sequence_name,
                    filter,
                ) {
                    sqls.push(sql);
                }
            }
        }

        for comment in &self.comments {
            if let Some(sql) = comment.to_sql(&self.database_name, &self.schema_name, "", filter) {
                sqls.push(sql);
            }
        }
        Ok(sqls)
    }

    fn create_schema_sql(&self) -> String {
        let schema = Self::quote(&self.schema_name);
        let schema_literal = Self::literal(&self.schema_name);
        let create_schema = format!(
            "IF SCHEMA_ID(N'{schema_literal}') IS NULL EXEC(N'CREATE SCHEMA {}')",
            Self::literal(&schema)
        );
        self.execute_in_database(&create_schema)
    }

    fn create_sequence_sql(&self, sequence: &MssqlSequence) -> String {
        let cycle = if sequence.is_cycling {
            "CYCLE"
        } else {
            "NO CYCLE"
        };
        let cache = if !sequence.is_cached {
            "NO CACHE".to_string()
        } else if let Some(cache_size) = sequence.cache_size {
            format!("CACHE {cache_size}")
        } else {
            "CACHE".to_string()
        };
        let sequence_name = format!(
            "{}.{}",
            Self::quote(&self.schema_name),
            Self::quote(&sequence.sequence_name)
        );
        let create = format!(
            "CREATE SEQUENCE {sequence_name} AS {} START WITH {} INCREMENT BY {} MINVALUE {} MAXVALUE {} {cycle} {cache}",
            sequence.data_type,
            sequence.start_value,
            sequence.increment,
            sequence.minimum_value,
            sequence.maximum_value,
        );
        self.execute_in_database(&create)
    }

    fn execute_in_database(&self, sql: &str) -> String {
        if self.database_name.is_empty() {
            sql.to_string()
        } else {
            format!(
                "EXEC {}.sys.sp_executesql N'{}'",
                Self::quote(&self.database_name),
                Self::literal(sql)
            )
        }
    }

    fn quote(identifier: &str) -> String {
        SqlUtil::escape_by_db_type(identifier, &DbType::Mssql)
    }

    fn literal(value: &str) -> String {
        value.replace('\'', "''")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::filter_config::FilterConfig;

    fn filter(do_structures: &str) -> RdbFilter {
        RdbFilter::from_config(
            &FilterConfig {
                do_structures: do_structures.to_string(),
                ..Default::default()
            },
            &DbType::Mssql,
        )
        .unwrap()
    }

    fn statement() -> MssqlCreateSchemaStatement {
        MssqlCreateSchemaStatement {
            database_name: "db]1".to_string(),
            schema_name: "schema.one".to_string(),
            sequences: vec![MssqlSequence {
                sequence_name: "seq'name".to_string(),
                data_type: "DECIMAL(10, 0)".to_string(),
                start_value: "100".to_string(),
                increment: "5".to_string(),
                minimum_value: "50".to_string(),
                maximum_value: "1000".to_string(),
                is_cycling: true,
                is_cached: true,
                cache_size: Some(20),
                comments: vec![MssqlComment::Sequence {
                    comment: "sequence's comment".to_string(),
                }],
            }],
            comments: vec![MssqlComment::Schema {
                comment: "schema's comment".to_string(),
            }],
        }
    }

    #[test]
    fn to_sqls_preserves_schema_sequence_options_and_comments() {
        let sqls = statement()
            .to_sqls(&filter("database,sequence,comment"))
            .unwrap();
        assert_eq!(sqls.len(), 4);
        assert_eq!(
            sqls[0].1,
            "EXEC [db]]1].sys.sp_executesql N'IF SCHEMA_ID(N''schema.one'') IS NULL EXEC(N''CREATE SCHEMA [schema.one]'')'"
        );
        assert_eq!(
            sqls[1].1,
            "EXEC [db]]1].sys.sp_executesql N'CREATE SEQUENCE [schema.one].[seq''name] AS DECIMAL(10, 0) START WITH 100 INCREMENT BY 5 MINVALUE 50 MAXVALUE 1000 CYCLE CACHE 20'"
        );
        assert_eq!(
            sqls[2].1,
            "EXEC [db]]1].sys.sp_addextendedproperty @name=N'MS_Description', @value=N'sequence''s comment', @level0type=N'SCHEMA', @level0name=N'schema.one', @level1type=N'SEQUENCE', @level1name=N'seq''name'"
        );
        assert_eq!(
            sqls[3].1,
            "EXEC [db]]1].sys.sp_addextendedproperty @name=N'MS_Description', @value=N'schema''s comment', @level0type=N'SCHEMA', @level0name=N'schema.one'"
        );
    }

    #[test]
    fn sequence_does_not_depend_on_database_structure_filter() {
        let sqls = statement().to_sqls(&filter("sequence,comment")).unwrap();
        assert_eq!(sqls.len(), 2);
        assert!(sqls[0].0.starts_with("sequence."));
        assert!(sqls[1].0.starts_with("sequence_comment."));
    }

    #[test]
    fn to_sqls_preserves_no_cache() {
        let statement = MssqlCreateSchemaStatement {
            database_name: String::new(),
            schema_name: "dbo".to_string(),
            sequences: vec![MssqlSequence {
                sequence_name: "descending".to_string(),
                data_type: "INT".to_string(),
                start_value: "-1".to_string(),
                increment: "-2".to_string(),
                minimum_value: "-101".to_string(),
                maximum_value: "-1".to_string(),
                is_cycling: false,
                is_cached: false,
                cache_size: None,
                comments: Vec::new(),
            }],
            comments: Vec::new(),
        };

        assert_eq!(
            statement.to_sqls(&filter("sequence")).unwrap()[0].1,
            "CREATE SEQUENCE [dbo].[descending] AS INT START WITH -1 INCREMENT BY -2 MINVALUE -101 MAXVALUE -1 NO CYCLE NO CACHE"
        );
    }
}

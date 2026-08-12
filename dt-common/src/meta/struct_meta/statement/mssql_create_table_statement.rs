use crate::{
    meta::struct_meta::structure::{
        comment::Comment, constraint::Constraint, index::Index, table::Table,
    },
    rdb_filter::RdbFilter,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlIdentityColumn {
    pub column_name: String,
    pub seed_value: String,
    pub increment_value: String,
    pub is_not_for_replication: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlComputedColumn {
    pub column_name: String,
    pub definition: String,
    pub is_persisted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlDefaultConstraint {
    pub constraint_name: String,
    pub column_name: String,
    pub definition: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlIndexColumn {
    pub column_name: String,
    pub key_ordinal: u32,
    pub is_descending_key: bool,
    pub is_included_column: bool,
}

#[derive(Debug, Clone)]
pub struct MssqlIndex {
    pub index: Index,
    pub index_type_desc: String,
    pub is_primary_key: bool,
    pub is_unique_constraint: bool,
    pub filter_definition: Option<String>,
    pub columns: Vec<MssqlIndexColumn>,
}

#[derive(Debug, Clone)]
pub struct MssqlCreateTableStatement {
    pub table: Table,
    pub identity_columns: Vec<MssqlIdentityColumn>,
    pub computed_columns: Vec<MssqlComputedColumn>,
    pub default_constraints: Vec<MssqlDefaultConstraint>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<MssqlIndex>,
    pub comments: Vec<Comment>,
}

impl MssqlCreateTableStatement {
    pub fn route(&mut self, dst_schema: &str, dst_tb: &str) {
        self.table.schema_name = dst_schema.to_string();
        self.table.table_name = dst_tb.to_string();

        for constraint in &mut self.constraints {
            constraint.schema_name = dst_schema.to_string();
            constraint.table_name = dst_tb.to_string();
        }

        for index in &mut self.indexes {
            index.index.schema_name = dst_schema.to_string();
            index.index.table_name = dst_tb.to_string();
        }

        for comment in &mut self.comments {
            comment.schema_name = dst_schema.to_string();
            comment.table_name = dst_tb.to_string();
        }
    }

    pub fn to_sqls(&mut self, _filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        todo!("generate MSSQL CREATE TABLE, constraint, index, and comment statements")
    }
}

use crate::meta::struct_meta::structure::column::ColumnDefault;
use crate::{config::config_enums::DbType, rdb_filter::RdbFilter};

use crate::meta::struct_meta::structure::index::IndexType;
use crate::meta::struct_meta::structure::{
    column::Column,
    constraint::Constraint,
    index::{Index, IndexKind},
    structure_type::StructureType,
    table::Table,
};

#[derive(Debug, Clone)]
pub struct MysqlCreateTableStatement {
    pub table: Table,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Index>,
}

impl MysqlCreateTableStatement {
    pub fn route(&mut self, dst_db: &str, dst_tb: &str) {
        self.table.database_name = dst_db.to_string();
        self.table.table_name = dst_tb.to_string();

        for index in self.indexes.iter_mut() {
            index.database_name = dst_db.to_string();
            index.table_name = dst_tb.to_string();
        }

        for constraint in self.constraints.iter_mut() {
            constraint.database_name = dst_db.to_string();
            constraint.table_name = dst_tb.to_string();
        }
    }

    pub fn to_sqls(&mut self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        let mut sqls = Vec::new();

        if !filter.filter_structure(&StructureType::Table) {
            let key = format!(
                "table.{}.{}",
                self.table.database_name, self.table.table_name
            );
            sqls.push((key, Self::table_to_sql(&mut self.table)));
        }

        if !self.indexes.is_empty() {
            let mut idx_appends = Vec::new();
            for i in self.indexes.iter_mut() {
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

                match i.index_type {
                    // join Btree index for same table
                    IndexType::Btree => {
                        idx_appends.push(Self::index_to_sql_appends(i));
                    }
                    _ => {
                        let standalone_key = format!(
                            "index.{}.{}.{}",
                            i.database_name, i.table_name, i.index_name
                        );
                        sqls.push((standalone_key, Self::index_to_sql(i)))
                    }
                }
            }
            if !idx_appends.is_empty() {
                let key = format!(
                    "index.{}.{}",
                    self.indexes[0].database_name, self.indexes[0].table_name
                );
                sqls.push((
                    key,
                    format!(
                        "ALTER TABLE `{}`.`{}` {}",
                        self.table.database_name,
                        self.table.table_name,
                        idx_appends.join(",")
                    ),
                ))
            }
        }

        if !filter.filter_structure(&StructureType::Constraint) {
            for i in self.constraints.iter() {
                let key = format!(
                    "constraint.{}.{}.{}",
                    i.database_name, i.table_name, i.constraint_name
                );
                sqls.push((key, Self::constraint_to_sql(i)));
            }
        }

        Ok(sqls)
    }

    fn table_to_sql(table: &mut Table) -> String {
        let (columns_sql, pks) = Self::columns_to_sql(&mut table.columns);
        let mut pk_str = String::new();
        if !pks.is_empty() {
            pk_str = format!(
                ", PRIMARY KEY ({})",
                pks.iter()
                    .map(|x| format!("`{}`", x))
                    .collect::<Vec<String>>()
                    .join(",")
            )
        }

        // Todo: table partition; column visible, generated(information_schema.column.GENERATION_EXPRESSION)
        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({}{})",
            table.database_name, table.table_name, columns_sql, pk_str
        );

        if !table.engine_name.is_empty() {
            sql = format!("{} ENGINE={} ", sql, table.engine_name);
        }

        if !table.character_set.is_empty() {
            sql = format!("{} DEFAULT CHARSET={}", sql, table.character_set);
        }

        if !table.table_collation.is_empty() {
            sql = format!("{} COLLATE={}", sql, table.table_collation);
        }

        if !table.table_comment.is_empty() {
            sql = format!("{} COMMENT='{}'", sql, Self::escape(&table.table_comment));
        }

        sql
    }

    fn columns_to_sql(columns: &mut [Column]) -> (String, Vec<String>) {
        let (mut sql_lines, mut pks) = (Vec::new(), Vec::new());

        columns.sort_by(|c1, c2| c1.ordinal_position.cmp(&c2.ordinal_position));
        for i in columns.iter() {
            let mut line = String::new();
            line.push_str(&format!("`{}` {}", i.column_name, i.column_type));

            if !i.character_set_name.is_empty() {
                line.push_str(&format!(" CHARACTER SET {}", i.character_set_name))
            }

            if !i.collation_name.is_empty() {
                line.push_str(&format!(" COLLATE {}", i.collation_name))
            }

            match &i.column_default {
                Some(ColumnDefault::Expression(v)) => line.push_str(&format!(" DEFAULT {}", v)),
                Some(ColumnDefault::Literal(v)) => {
                    if i.column_type.to_lowercase().starts_with("bit") {
                        // https://github.com/apecloud/ape-dts/issues/319
                        // CREATE TABLE a(b bit(1) default b'1');
                        line.push_str(&format!(" DEFAULT {}", v))
                    } else {
                        line.push_str(&format!(" DEFAULT '{}'", Self::escape(v)))
                    }
                }
                _ => {}
            }

            // auto_increment
            // on update CURRENT_TIMESTAMP
            // mysql 8.0:
            //  DEFAULT_GENERATED
            //  DEFAULT_GENERATED on update CURRENT_TIMESTAMP
            let extra = i.extra.replacen("DEFAULT_GENERATED", "", 1);
            if !extra.is_empty() {
                line.push_str(&format!(" {}", extra));
            }

            let nullable = if !i.is_nullable {
                String::from("NOT NULL")
            } else {
                String::from("NULL")
            };

            if !i.column_comment.is_empty() {
                line.push_str(&format!(" COMMENT '{}'", Self::escape(&i.column_comment)))
            }

            line.push_str(&format!(" {}", nullable));
            sql_lines.push(line);

            if i.column_key == "PRI" {
                pks.push(i.column_name.clone());
            }
        }

        (sql_lines.join(", "), pks)
    }

    fn index_to_sql(index: &mut Index) -> String {
        let columns_sql = Self::build_cols_for_index(index);

        // no need index_type in "CREATE {} INDEX `{}` USING {IndexType}"
        // since only BETREE supported in both InnoDB and MyISAM
        // refer: https://dev.mysql.com/doc/refman/8.0/en/create-index.html
        let mut sql: String = format!(
            "CREATE {} INDEX `{}` ON `{}`.`{}` ({}) ",
            index.index_kind, index.index_name, index.database_name, index.table_name, columns_sql
        );

        if !index.comment.is_empty() {
            sql.push_str(&format!("COMMENT '{}' ", Self::escape(&index.comment)));
        }

        sql
    }

    fn index_to_sql_appends(index: &mut Index) -> String {
        let columns_sql = Self::build_cols_for_index(index);

        // no need index_type in "CREATE {} INDEX `{}` USING {IndexType}"
        // since only BETREE supported in both InnoDB and MyISAM
        // refer: https://dev.mysql.com/doc/refman/8.0/en/create-index.html
        let mut sql: String = format!(
            "ADD {} INDEX `{}` ({}) ",
            index.index_kind, index.index_name, columns_sql
        );

        if !index.comment.is_empty() {
            sql.push_str(&format!("COMMENT '{}' ", index.comment));
        }

        sql
    }

    fn build_cols_for_index(index: &mut Index) -> String {
        index
            .columns
            .sort_by(|a, b| a.seq_in_index.cmp(&b.seq_in_index));
        index
            .columns
            .iter()
            .filter(|x| !x.column_name.is_empty())
            .map(|x| {
                if let Some(prefix_length) = x.prefix_length {
                    format!("`{}`({})", x.column_name, prefix_length)
                } else {
                    format!("`{}`", x.column_name)
                }
            })
            .collect::<Vec<String>>()
            .join(",")
    }

    fn constraint_to_sql(constraint: &Constraint) -> String {
        // TODO, check for escapes
        format!(
            "ALTER TABLE `{}`.`{}` ADD CONSTRAINT `{}` {} {} ",
            constraint.database_name,
            constraint.table_name,
            constraint.constraint_name,
            constraint.constraint_type.to_str(DbType::Mysql),
            constraint.definition
        )
    }

    fn escape(text: &str) -> String {
        text.replace('\'', "\'\'").to_string()
    }
}

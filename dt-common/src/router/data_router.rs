use std::collections::HashMap;

use anyhow::{ensure, Result};

use super::DtRouter;
use crate::{
    config::config_enums::DbType,
    meta::{
        col_value::ColValue,
        ddl_meta::{ddl_data::DdlData, ddl_statement::DdlStatement},
        row_data::RowData,
        struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
    },
    resource::{ResourceKind, RID},
    rid,
};

impl DtRouter {
    pub fn column_map(
        &self,
        db: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<HashMap<String, String>>> {
        let id = if self.src_db_type() == &DbType::Mssql {
            rid!(db, schema, table)
        } else {
            rid!(schema, table)
        };
        self.column_names(&id, false)
    }

    pub fn reverse_column_map(
        &self,
        db: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<HashMap<String, String>>> {
        let id = if self.route_db_type() == &DbType::Mssql {
            rid!(db, schema, table)
        } else {
            rid!(schema, table)
        };
        self.column_names(&id, true)
    }

    fn column_names(&self, table: &RID, reverse: bool) -> Result<Option<HashMap<String, String>>> {
        let Some(router) = self.resource_routers().get(&ResourceKind::Column) else {
            return Ok(None);
        };
        let source = if reverse {
            self.reverse::<RID>(table)?
        } else {
            table.clone()
        };
        let mut columns = HashMap::new();
        for (from, to) in router.mappings() {
            if from.parent().as_ref() == Some(&source) {
                let from = from.segments().last().cloned().unwrap_or_default();
                let to = to.segments().last().cloned().unwrap_or_default();
                if reverse {
                    columns.insert(to, from);
                } else {
                    columns.insert(from, to);
                }
            }
        }
        Ok((!columns.is_empty()).then_some(columns))
    }

    pub fn route_row(&self, row: RowData) -> Result<RowData> {
        self.map_row(row, false)
    }

    pub fn reverse_route_row(&self, row: RowData) -> Result<RowData> {
        self.map_row(row, true)
    }

    fn map_row(&self, mut row: RowData, reverse: bool) -> Result<RowData> {
        let input_db_type = if reverse {
            self.route_db_type()
        } else {
            self.src_db_type()
        };
        let source = if input_db_type == &DbType::Mssql {
            rid!(row.db, row.schema, row.tb)
        } else {
            rid!(row.schema, row.tb)
        };
        let route = |id: &RID| {
            if reverse {
                self.reverse::<RID>(id)
            } else {
                self.forward::<RID>(id)
            }
        };
        let target = route(&source)?;
        let db_type = if reverse {
            self.src_db_type()
        } else {
            self.route_db_type()
        };
        (row.db, row.schema, row.tb) = resource_parts(target, db_type);
        let Some(columns) = self.column_names(&source, reverse)? else {
            return Ok(row);
        };
        let map_values = |values: HashMap<String, ColValue>| -> Result<HashMap<String, ColValue>> {
            let mut mapped = HashMap::new();
            for (name, value) in values {
                let name = columns.get(&name).cloned().unwrap_or(name);
                ensure!(
                    mapped.insert(name.clone(), value).is_none(),
                    "column route collision: {name}"
                );
            }
            Ok(mapped)
        };
        row.before = row.before.map(map_values).transpose()?;
        row.after = row.after.map(map_values).transpose()?;
        Ok(row)
    }

    pub fn route_ddl(&self, mut ddl_data: DdlData) -> Result<DdlData> {
        let src_db = ddl_data.default_db.clone();
        let route = |schema: &str, table: &str| -> Result<(String, String, String)> {
            let source = if self.src_db_type() == &DbType::Mssql {
                rid!(src_db, schema, table)
            } else {
                rid!(schema, table)
            };
            Ok(resource_parts(self.forward(source)?, self.route_db_type()))
        };
        let has_rename_target = !ddl_data.get_rename_to_schema_tb().1.is_empty();
        match &mut ddl_data.statement {
            DdlStatement::MysqlAlterTableRename(_)
            | DdlStatement::PgAlterTableRename(_)
            | DdlStatement::RenameTable(_)
            | DdlStatement::MongoCommand(_)
                if has_rename_target =>
            {
                let (_, src_schema, src_tb) = ddl_data.get_db_schema_tb();
                let (_, src_new_schema, src_new_tb) = ddl_data.get_rename_to_db_schema_tb();
                let (_, dst_schema, dst_tb) = route(&src_schema, &src_tb)?;
                let (_, dst_new_schema, dst_new_tb) = route(&src_new_schema, &src_new_tb)?;
                ddl_data.statement.route_rename_table(
                    dst_schema,
                    dst_tb,
                    dst_new_schema,
                    dst_new_tb,
                );
            }

            _ => {
                let (_, src_schema, src_tb) = ddl_data.get_db_schema_tb();
                let (_, dst_schema, dst_tb) = route(&src_schema, &src_tb)?;
                ddl_data.statement.route(dst_schema, dst_tb);
            }
        }

        if src_db.is_empty() && !ddl_data.default_schema.is_empty() {
            ddl_data.default_schema = self.forward(rid!(ddl_data.default_schema))?;
        } else if !src_db.is_empty() {
            ddl_data.default_db = self.forward(rid!(src_db))?;
        }
        Ok(ddl_data)
    }

    pub fn route_struct(&self, mut struct_data: StructData) -> Result<StructData> {
        let src_db = struct_data.db.clone();
        let route = |schema: &str, table: &str| -> Result<(String, String, String)> {
            let source = if self.src_db_type() == &DbType::Mssql {
                rid!(src_db, schema, table)
            } else {
                rid!(schema, table)
            };
            Ok(resource_parts(self.forward(source)?, self.route_db_type()))
        };
        let mut dst_db = src_db.clone();
        match &mut struct_data.statement {
            StructStatement::MysqlCreateTable(s) => {
                let (schema, tb) = (s.table.database_name.clone(), s.table.table_name.clone());
                let (mapped_db, dst_schema, dst_tb) = route(&schema, &tb)?;
                dst_db = mapped_db;
                s.route(&dst_schema, &dst_tb)
            }

            StructStatement::MysqlCreateDatabase(s) => {
                let src_schema = s.database.name.clone();
                let dst_schema = self.forward::<String>(rid!(src_schema))?;
                s.route(&dst_schema)
            }

            StructStatement::MongoCreateCollection(s) => {
                let (schema, tb) = (s.database_name.clone(), s.collection_name.clone());
                let (mapped_db, dst_schema, dst_tb) = route(&schema, &tb)?;
                dst_db = mapped_db;
                s.route(&dst_schema, &dst_tb)
            }

            StructStatement::MongoShardKey(s) => {
                let ns = s.shard_collection.ns.clone();
                if let Some((schema, tb)) = ns.split_once('.') {
                    let (mapped_db, dst_schema, dst_tb) = route(schema, tb)?;
                    dst_db = mapped_db;
                    s.route(schema, tb, &dst_schema, &dst_tb)
                }
            }

            StructStatement::PgCreateTable(s) => {
                let (schema, tb) = (s.table.schema_name.clone(), s.table.table_name.clone());
                let (mapped_db, dst_schema, dst_tb) = route(&schema, &tb)?;
                dst_db = mapped_db;
                s.route(&dst_schema, &dst_tb)
            }

            StructStatement::PgCreateSchema(s) => {
                let src_schema = s.schema.name.clone();
                let dst_schema = self.forward::<String>(rid!(src_schema))?;
                s.route(&dst_schema)
            }

            _ => {}
        }

        struct_data.db = dst_db;
        Ok(struct_data)
    }
}

fn resource_parts(id: RID, db_type: &DbType) -> (String, String, String) {
    if *db_type == DbType::Mssql {
        id.into()
    } else {
        let (schema, table) = id.into();
        (String::new(), schema, table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::config_enums::DbType,
        meta::{
            ddl_meta::ddl_parser::DdlParser,
            row_type::RowType,
            struct_meta::{
                statement::mysql_create_database_statement::MysqlCreateDatabaseStatement,
                structure::database::Database,
            },
        },
        rid,
    };

    #[test]
    fn row_adapters_preserve_identity_columns_and_reverse_routes() {
        for (engine, db, schema) in [
            (DbType::Mysql, "", "inventory"),
            (DbType::Pg, "", "inventory"),
            (DbType::Mssql, "inventory", "sales"),
            (DbType::Mongo, "", "inventory"),
        ] {
            let source = if engine == DbType::Mssql {
                rid!(db, schema, "orders")
            } else {
                rid!(schema, "orders")
            };
            let target = source.replace_prefix(1, &rid!("archive"));
            let mut rules = vec![(rid!("inventory"), rid!("archive"))];
            let has_columns = engine != DbType::Mongo;
            if has_columns {
                rules.push((source.child("id"), target.child("order_id")));
            }
            let router = DtRouter::new(engine.clone(), engine.clone(), rules).unwrap();
            let values = HashMap::from([
                ("id".into(), ColValue::Long(42)),
                ("value".into(), ColValue::None),
            ]);
            let row = RowData::new(
                db.into(),
                schema.into(),
                "orders".into(),
                7,
                RowType::Update,
                Some(values.clone()),
                Some(values),
            );
            let routed = router.route_row(row.clone()).unwrap();
            let expected = if engine == DbType::Mssql {
                ("archive", "sales", "orders")
            } else {
                ("", "archive", "orders")
            };
            assert_eq!(
                (
                    routed.db.as_str(),
                    routed.schema.as_str(),
                    routed.tb.as_str()
                ),
                expected
            );
            assert_eq!(routed.chunk_id, 7);
            for values in [&routed.before, &routed.after] {
                assert!(values.as_ref().unwrap().contains_key(if has_columns {
                    "order_id"
                } else {
                    "id"
                }));
            }
            assert_eq!(router.reverse_route_row(routed).unwrap(), row, "{engine}");
            if has_columns {
                let mut collision = row;
                collision
                    .after
                    .as_mut()
                    .unwrap()
                    .insert("order_id".into(), ColValue::Long(99));
                assert!(router.route_row(collision).is_err());
            }
        }
    }

    #[test]
    fn ddl_and_structure_adapters_route_namespaces_and_both_rename_sides() {
        for (engine, query) in [
            (
                DbType::Mysql,
                "ALTER TABLE inventory.orders RENAME TO inventory.events",
            ),
            (DbType::Pg, "ALTER TABLE inventory.orders RENAME TO events"),
        ] {
            let router = DtRouter::new(
                engine.clone(),
                engine.clone(),
                [
                    (rid!("inventory"), rid!("archive")),
                    (rid!("inventory", "orders"), rid!("archive", "old_orders")),
                    (rid!("inventory", "events"), rid!("archive", "new_events")),
                ],
            )
            .unwrap();
            let mut ddl = DdlParser::new(engine).parse(query).unwrap().unwrap();
            ddl.default_schema = "inventory".into();
            let routed = router.route_ddl(ddl).unwrap();
            assert_eq!(
                routed.get_schema_tb(),
                ("archive".into(), "old_orders".into())
            );
            assert_eq!(
                routed.get_rename_to_schema_tb(),
                ("archive".into(), "new_events".into())
            );
        }
        let router = DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("inventory"), rid!("archive"))],
        )
        .unwrap();
        let ddl = DdlParser::new(DbType::Mysql)
            .parse("CREATE DATABASE inventory")
            .unwrap()
            .unwrap();
        let routed = router.route_ddl(ddl).unwrap();
        assert_eq!(routed.get_schema_tb().0, "archive");
        let routed = router
            .route_struct(StructData {
                db: String::new(),
                schema: "inventory".into(),
                tb: String::new(),
                statement: StructStatement::MysqlCreateDatabase(MysqlCreateDatabaseStatement {
                    database: Database {
                        name: "inventory".into(),
                        ..Default::default()
                    },
                }),
            })
            .unwrap();
        let StructStatement::MysqlCreateDatabase(statement) = routed.statement else {
            panic!("unexpected statement")
        };
        assert_eq!(statement.database.name, "archive");
    }

    #[test]
    fn table_adapters_preserve_namespace_depth_and_cross_engine_layouts() {
        for (src_engine, dst_engine, source, target, src_fields, dst_fields) in [
            (
                DbType::Pg,
                DbType::Pg,
                rid!("inventory"),
                rid!("archive"),
                ("", "inventory", ""),
                ("", "archive", ""),
            ),
            (
                DbType::Mssql,
                DbType::Mssql,
                rid!("inventory"),
                rid!("archive"),
                ("inventory", "", ""),
                ("archive", "", ""),
            ),
            (
                DbType::Mssql,
                DbType::Mssql,
                rid!("inventory", "sales"),
                rid!("archive", "history"),
                ("inventory", "sales", ""),
                ("archive", "history", ""),
            ),
            (
                DbType::Pg,
                DbType::Mssql,
                rid!("inventory", "orders"),
                rid!("archive", "history", "events"),
                ("", "inventory", "orders"),
                ("archive", "history", "events"),
            ),
            (
                DbType::Mssql,
                DbType::Mysql,
                rid!("inventory", "sales", "orders"),
                rid!("archive", "events"),
                ("inventory", "sales", "orders"),
                ("", "archive", "events"),
            ),
        ] {
            let router = DtRouter::new(src_engine, dst_engine, [(source.clone(), target)]).unwrap();
            let target: RID = router.forward(&source).unwrap();
            let routed = resource_parts(target.clone(), router.route_db_type());
            assert_eq!(
                (routed.0.as_str(), routed.1.as_str(), routed.2.as_str()),
                dst_fields
            );
            let reversed = resource_parts(router.reverse(target).unwrap(), router.src_db_type());
            assert_eq!(
                (
                    reversed.0.as_str(),
                    reversed.1.as_str(),
                    reversed.2.as_str()
                ),
                src_fields
            );
            if !src_fields.2.is_empty() {
                let row = RowData::new(
                    src_fields.0.into(),
                    src_fields.1.into(),
                    src_fields.2.into(),
                    0,
                    RowType::Insert,
                    None,
                    None,
                );
                let routed_row = router.route_row(row.clone()).unwrap();
                assert_eq!(
                    (
                        routed_row.db.clone(),
                        routed_row.schema.clone(),
                        routed_row.tb.clone()
                    ),
                    routed
                );
                assert_eq!(router.reverse_route_row(routed_row).unwrap(), row);
            }
        }
    }
}

use std::collections::HashMap;

use anyhow::{ensure, Result};

use super::DtRouter;
use crate::{
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
    pub fn route_row(&self, row: RowData) -> Result<RowData> {
        self.map_row(row, false)
    }

    pub fn reverse_route_row(&self, row: RowData) -> Result<RowData> {
        self.map_row(row, true)
    }

    pub fn route_ddl(&self, mut ddl_data: DdlData) -> Result<DdlData> {
        let src_db = ddl_data.default_db.clone();
        let source: RID = ddl_data.get_id();
        let has_rename_target = !ddl_data.statement.get_rename_to_schema_tb().1.is_empty();
        match &mut ddl_data.statement {
            DdlStatement::MysqlAlterTableRename(_)
            | DdlStatement::PgAlterTableRename(_)
            | DdlStatement::RenameTable(_)
            | DdlStatement::MongoCommand(_)
                if has_rename_target =>
            {
                let (dst_schema, dst_tb) = self.forward(source)?;
                let (dst_new_schema, dst_new_tb) = self.forward(ddl_data.get_rename_id::<RID>())?;
                ddl_data.statement.route_rename_table(
                    dst_schema,
                    dst_tb,
                    dst_new_schema,
                    dst_new_tb,
                );
            }

            _ => {
                let (dst_schema, dst_tb) = if ddl_data.statement.get_schema_tb().1.is_empty() {
                    (self.forward::<String>(source)?, String::new())
                } else {
                    self.forward(source)?
                };
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
        let mut dst_db = src_db.clone();
        match &mut struct_data.statement {
            StructStatement::MysqlCreateTable(s) => {
                let (schema, tb) = (s.table.database_name.clone(), s.table.table_name.clone());
                let (mapped_db, dst_schema, dst_tb): (String, String, String) =
                    self.forward(rid!(src_db, schema, tb))?;
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
                let (mapped_db, dst_schema, dst_tb): (String, String, String) =
                    self.forward(rid!(src_db, schema, tb))?;
                dst_db = mapped_db;
                s.route(&dst_schema, &dst_tb)
            }

            StructStatement::MongoShardKey(s) => {
                let ns = s.shard_collection.ns.clone();
                if let Some((schema, tb)) = ns.split_once('.') {
                    let (mapped_db, dst_schema, dst_tb): (String, String, String) =
                        self.forward(rid!(src_db, schema, tb))?;
                    dst_db = mapped_db;
                    s.route(schema, tb, &dst_schema, &dst_tb)
                }
            }

            StructStatement::PgCreateTable(s) => {
                let (schema, tb) = (s.table.schema_name.clone(), s.table.table_name.clone());
                let (mapped_db, dst_schema, dst_tb): (String, String, String) =
                    self.forward(rid!(src_db, schema, tb))?;
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

    /// Source column prefix, e.g. `rid!("inventory", "orders")` for MySQL.
    pub fn column_map(&self, prefix: impl Into<RID>) -> Option<HashMap<String, String>> {
        self.column_names(prefix.into(), false)
    }

    /// Routed column prefix; returns target column names mapped to source names.
    pub fn reverse_column_map(&self, prefix: impl Into<RID>) -> Option<HashMap<String, String>> {
        self.column_names(prefix.into(), true)
    }

    fn map_row(&self, mut row: RowData, reverse: bool) -> Result<RowData> {
        let source = rid!(row.db, row.schema, row.tb);
        (row.db, row.schema, row.tb) = if reverse {
            self.reverse(&source)?
        } else {
            self.forward(&source)?
        };
        let columns = if reverse {
            self.reverse_column_map(source)
        } else {
            self.column_map(source)
        };
        let Some(columns) = columns else {
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

    fn column_names(&self, prefix: RID, reverse: bool) -> Option<HashMap<String, String>> {
        let router = self.resource_routers().get(&ResourceKind::Column)?;
        let columns: HashMap<_, _> = router
            .mappings()
            .iter()
            .filter_map(|(source, target)| {
                let (from, to) = if reverse {
                    (target, source)
                } else {
                    (source, target)
                };
                let (name, parent) = from.segments().split_last()?;
                if parent != prefix.segments() {
                    return None;
                }
                Some((name.clone(), to.segments().last()?.clone()))
            })
            .collect();
        (!columns.is_empty()).then_some(columns)
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
            let source = rid!(db, schema, "orders");
            let target = source.replace_prefix(1, &rid!("archive"));
            let mut rules = vec![(rid!("inventory"), rid!("archive"))];
            let has_columns = engine != DbType::Mongo;
            if has_columns {
                rules.push((source.child("id"), target.child("order_id")));
            }
            let router = DtRouter::new(engine.clone(), engine.clone(), rules).unwrap();
            assert_eq!(
                router.has_resource_router(ResourceKind::Column),
                has_columns
            );
            assert_eq!(
                router.column_map(&source),
                has_columns.then(|| HashMap::from([("id".into(), "order_id".into())]))
            );
            assert_eq!(
                router.reverse_column_map(target.clone()),
                has_columns.then(|| HashMap::from([("order_id".into(), "id".into())]))
            );
            for prefix in [target.clone(), source.prefix(1), source.child("id")] {
                assert_eq!(router.column_map(prefix), None);
            }
            for prefix in [source.clone(), target.prefix(1), target.child("order_id")] {
                assert_eq!(router.reverse_column_map(prefix), None);
            }
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
                assert_eq!(values.as_ref().unwrap().get("value"), Some(&ColValue::None));
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
                routed.get_id::<(String, String)>(),
                ("archive".into(), "old_orders".into())
            );
            assert_eq!(
                routed.get_rename_id::<(String, String)>(),
                ("archive".into(), "new_events".into())
            );
        }
        for (engine, query) in [
            (DbType::Mysql, "CREATE DATABASE inventory"),
            (DbType::Mysql, "DROP DATABASE inventory"),
            (DbType::Pg, "CREATE SCHEMA inventory"),
            (DbType::Pg, "DROP SCHEMA inventory"),
        ] {
            let router = DtRouter::new(
                engine.clone(),
                engine.clone(),
                [(rid!("inventory"), rid!("archive"))],
            )
            .unwrap();
            let ddl = DdlParser::new(engine).parse(query).unwrap().unwrap();
            let routed = router.route_ddl(ddl).unwrap();
            assert_eq!(routed.get_id::<RID>(), rid!("archive"), "{query}");
        }
        let router = DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("inventory"), rid!("archive"))],
        )
        .unwrap();
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
    fn table_adapters_preserve_cross_engine_layouts() {
        for (src_engine, dst_engine, source, target, src_fields, dst_fields) in [
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
            let rules = [
                (source.clone(), target.clone()),
                (source.child("id"), target.child("order_id")),
            ];
            let router = DtRouter::new(src_engine, dst_engine, rules).unwrap();
            assert_eq!(router.forward::<RID>(&source).unwrap(), target);
            assert_eq!(router.reverse::<RID>(target).unwrap(), source);
            let row = RowData::new(
                src_fields.0.into(),
                src_fields.1.into(),
                src_fields.2.into(),
                0,
                RowType::Insert,
                Some(HashMap::from([("id".into(), ColValue::Long(1))])),
                Some(HashMap::from([("id".into(), ColValue::Long(2))])),
            );
            let routed_row = router.route_row(row.clone()).unwrap();
            assert_eq!(
                (
                    routed_row.db.as_str(),
                    routed_row.schema.as_str(),
                    routed_row.tb.as_str()
                ),
                dst_fields
            );
            assert_eq!(
                routed_row.before.as_ref().unwrap()["order_id"],
                ColValue::Long(1)
            );
            assert_eq!(
                routed_row.after.as_ref().unwrap()["order_id"],
                ColValue::Long(2)
            );
            assert_eq!(router.reverse_route_row(routed_row).unwrap(), row);
        }
    }
}

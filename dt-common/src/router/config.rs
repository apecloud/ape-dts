use std::collections::HashMap;

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

use super::Router;
use crate::{
    config::{
        config_enums::DbType,
        config_token_parser::ConfigTokenParser,
        router_config::{BaseRouterConfig, RouterConfig},
    },
    resource::{config, resource_depth, resource_levels, validate_rid, ResourceKind, RID},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteRule {
    from: RID,
    to: RID,
}

fn routes(
    value: &str,
    src: &DbType,
    dst: &DbType,
    legacy: bool,
    table: bool,
) -> Result<Vec<(RID, RID)>> {
    if value.trim().starts_with("json:") {
        return Ok(config::json::<Vec<RouteRule>>(value)?
            .into_iter()
            .map(|r| (r.from, r.to))
            .collect());
    }
    if legacy && !table && matches!(src, DbType::Mssql) && matches!(dst, DbType::Mssql) {
        // MSSQL's existing database map permits unquoted dots inside database names.
        let tokens =
            ConfigTokenParser::parse_config_with_delimiters(value.trim(), src, &[',', ':'], None)?;
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        return tokens
            .split(|token| token == ",")
            .map(|entry| {
                ensure!(
                    entry.len() == 3 && entry[1] == ":",
                    "expected source:target database mapping"
                );
                Ok((
                    config::exact_path(vec![entry[0].clone()], src, true)?,
                    config::exact_path(vec![entry[2].clone()], dst, true)?,
                ))
            })
            .collect();
    }
    config::pairs(value, src, dst)?
        .into_iter()
        .map(|(from, to)| {
            let (from, to) = if legacy && table {
                (
                    config::legacy_table(from, src)?,
                    config::legacy_table(to, dst)?,
                )
            } else {
                (from, to)
            };
            Ok((
                config::exact_path(from, src, legacy)?,
                config::exact_path(to, dst, legacy)?,
            ))
        })
        .collect()
}

fn resolve(
    old: Vec<(RID, RID)>,
    new: Option<&str>,
    kind: ResourceKind,
    src: &DbType,
    dst: &DbType,
) -> Result<Vec<(RID, RID)>> {
    // Old maps are last-value-wins, matching the original config's HashMap construction.
    let old: HashMap<_, _> = old.into_iter().collect();
    let rules = if let Some(value) = new {
        let new = routes(value, src, dst, false, false)?;
        if !old.is_empty() {
            let new_map: HashMap<_, _> = new.iter().cloned().collect();
            ensure!(
                old == new_map,
                "conflicting legacy and {kind:?} router rules"
            );
        }
        new
    } else {
        old.into_iter().collect()
    };
    for (from, _) in &rules {
        validate_rid(src, kind, from)?;
    }
    Ok(rules)
}

#[derive(Deserialize)]
struct ColumnRule {
    db: String,
    tb: String,
    col_map: HashMap<String, String>,
}

impl Router {
    /// Compile raw resource routing rules and optional legacy [router] mappings.
    pub fn from_config(
        bases: &HashMap<ResourceKind, BaseRouterConfig>,
        legacy: &RouterConfig,
        src: DbType,
        dst: DbType,
    ) -> Result<Self> {
        for kind in bases.keys() {
            ensure!(
                resource_depth(&src, *kind).is_some(),
                "unsupported {kind:?} for {src}"
            );
        }
        let RouterConfig::Rdb {
            schema_map,
            tb_map,
            col_map,
            ..
        } = legacy;
        if matches!(src, DbType::Kafka) {
            ensure!(
                schema_map.trim().is_empty(),
                "Kafka does not support [router].db_map; use [filter.topic].router"
            );
        }
        let mut rules = Vec::new();
        for (index, &kind) in resource_levels(&src).iter().enumerate() {
            if kind == ResourceKind::Column {
                continue;
            }
            let key = if index == 0 {
                Some(("db_map", schema_map))
            } else if matches!(kind, ResourceKind::Table | ResourceKind::Collection) {
                Some(("tb_map", tb_map))
            } else {
                None
            };
            let old = if let Some((key, value)) = key {
                routes(value, &src, &dst, true, key == "tb_map")
                    .with_context(|| format!("[router].{key}"))?
            } else {
                Vec::new()
            };
            rules.extend(resolve(
                old,
                bases.get(&kind).and_then(|base| base.router.as_deref()),
                kind,
                &src,
                &dst,
            )?);
        }
        if matches!(src, DbType::Redis | DbType::Kafka) {
            ensure!(
                tb_map.trim().is_empty(),
                "{src} does not support [router].tb_map"
            );
        }
        let mut old_columns = Vec::new();
        if !col_map.trim().is_empty() {
            ensure!(
                resource_depth(&src, ResourceKind::Column).is_some(),
                "{src} does not support column routing"
            );
            let parent_router = Self::new(src.clone(), dst.clone(), rules.clone())?;
            let mut tables = HashMap::new();
            for entry in config::json::<Vec<ColumnRule>>(col_map).context("[router].col_map")? {
                let source_table = config::legacy_json_table(&entry.db, &entry.tb, &src)?;
                tables.insert(source_table, entry.col_map);
            }
            for (source_table, columns) in tables {
                let target_table = parent_router.route_rid(&source_table)?;
                for (from, to) in columns {
                    old_columns.push((source_table.child(from)?, target_table.child(to)?));
                }
            }
        }
        if resource_depth(&src, ResourceKind::Column).is_some() {
            rules.extend(resolve(
                old_columns,
                bases
                    .get(&ResourceKind::Column)
                    .and_then(|base| base.router.as_deref()),
                ResourceKind::Column,
                &src,
                &dst,
            )?);
        }
        Self::new(src, dst, rules)
    }
}

#[cfg(test)]
mod tests {
    use configparser::ini::Ini;

    use super::*;
    use crate::rid;

    fn load_router(ini: &Ini, src: DbType, dst: DbType) -> Result<Router> {
        let bases = BaseRouterConfig::from_ini(ini, &src)?;
        Router::from_config(&bases, &RouterConfig::from_ini(ini), src, dst)
    }

    fn ini(text: &str) -> Ini {
        let mut ini = Ini::new();
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(text.into()).unwrap();
        ini
    }

    #[test]
    fn from_config_validates_resource_maps_and_compiles_columns_after_parents() {
        let mut bases = HashMap::from([
            (
                ResourceKind::Column,
                BaseRouterConfig {
                    router: Some("db.t.id:archive.orders.order_id".into()),
                },
            ),
            (
                ResourceKind::Table,
                BaseRouterConfig {
                    router: Some("db.t:archive.orders".into()),
                },
            ),
        ]);
        let legacy = RouterConfig::Rdb {
            schema_map: String::new(),
            tb_map: String::new(),
            col_map: String::new(),
            topic_map: String::new(),
        };
        let router = Router::from_config(&bases, &legacy, DbType::Mysql, DbType::Pg).unwrap();
        assert_eq!(
            router.route(&rid!("db", "t", "id")).unwrap(),
            rid!("archive", "orders", "order_id")
        );
        assert_eq!(
            router.route(&rid!("db", "t", "other")).unwrap(),
            rid!("archive", "orders", "other")
        );
        bases.insert(
            ResourceKind::Schema,
            BaseRouterConfig {
                router: Some("db.schema:archive".into()),
            },
        );
        assert!(Router::from_config(&bases, &legacy, DbType::Mysql, DbType::Pg).is_err());
        let mut bases = HashMap::from([(ResourceKind::Table, BaseRouterConfig::default())]);
        let legacy = RouterConfig::Rdb {
            schema_map: String::new(),
            tb_map: "db.t:archive.orders".into(),
            col_map: String::new(),
            topic_map: String::new(),
        };
        assert_eq!(
            Router::from_config(&bases, &legacy, DbType::Mysql, DbType::Pg)
                .unwrap()
                .route(&rid!("db", "t"))
                .unwrap(),
            rid!("archive", "orders")
        );
        bases.get_mut(&ResourceKind::Table).unwrap().router = Some(String::new());
        assert!(Router::from_config(&bases, &legacy, DbType::Mysql, DbType::Pg).is_err());
    }

    #[test]
    fn ini_handles_endpoint_dialects_json_and_legacy_column_parents() {
        let r = load_router(
            &ini("[filter.table]\nrouter=`a.b`.`order`:\"archive\".\"order.2026\""),
            DbType::Mysql,
            DbType::Pg,
        )
        .unwrap();
        assert_eq!(
            r.route(&rid!("a.b", "order")).unwrap(),
            rid!("archive", "order.2026")
        );
        let r = load_router(
            &ini(r#"[filter.table]
    router=json:[{"from":["a.b","order"],"to":["archive","order.2026"]}]
    "#),
            DbType::Mysql,
            DbType::Pg,
        )
        .unwrap();
        assert_eq!(
            r.route(&rid!("a.b", "order")).unwrap(),
            rid!("archive", "order.2026")
        );
        let r = load_router(
            &ini(r#"[router]
    db_map=market:archive
    tb_map=market.order:archive.history.order_2026
    col_map=json:[{"db":"market","tb":"order","col_map":{"value":"old_value"}},{"db":"market","tb":"order","col_map":{"id":"order_id"}}]
    "#),
            DbType::Mssql,
            DbType::Mssql,
        )
        .unwrap();
        assert_eq!(
            r.route(&rid!("market", "dbo", "order", "id")).unwrap(),
            rid!("archive", "history", "order_2026", "order_id")
        );
        assert_eq!(
            r.route(&rid!("market", "dbo", "order", "value")).unwrap(),
            rid!("archive", "history", "order_2026", "value")
        );
        let dotted = load_router(
            &ini("[router]\ndb_map=source.db:target.db"),
            DbType::Mssql,
            DbType::Mssql,
        )
        .unwrap();
        assert_eq!(
            dotted.route(&rid!("source.db", "dbo", "t")).unwrap(),
            rid!("target.db", "dbo", "t")
        );
        for text in [
            "[filter.table]\nrouter=a.t:b.t:c.t",
            "[filter.table]\nrouter=a.*:b.t",
            "[filter.table]\nrouter=a.t:b.t,",
            "[router]\ntb_map=a.t:b.t\n[filter.table]\nrouter=a.t:c.t",
        ] {
            let ini = ini(text);
            let bases = BaseRouterConfig::from_ini(&ini, &DbType::Mysql).unwrap();
            assert!(
                Router::from_config(
                    &bases,
                    &RouterConfig::from_ini(&ini),
                    DbType::Mysql,
                    DbType::Mysql
                )
                .is_err(),
                "{text}"
            );
        }
    }
}

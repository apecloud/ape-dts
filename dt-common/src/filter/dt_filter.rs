use std::collections::{HashMap, HashSet};

use anyhow::Result;
use dashmap::DashMap;
use strum::IntoEnumIterator;

use super::{CommonFilter, CommonFilterKind, ExpressionFilter, ResourceFilter};
use crate::{
    config::config_enums::DbType,
    meta::{
        dcl_meta::dcl_type::DclType, ddl_meta::ddl_type::DdlType, row_type::RowType,
        struct_meta::structure::structure_type::StructureType,
    },
    resource::{PatternMatcher, ResourceKind, RID, WILDCARD},
    utils::sql_util::SqlUtil,
};

#[derive(Debug, Clone)]
pub struct DtFilter {
    pub db_type: DbType,
    pub matcher: PatternMatcher,
    resource_filters: HashMap<ResourceKind, ResourceFilter>,
    common_filters: HashMap<CommonFilterKind, CommonFilter>,
    pub expression_filters: HashMap<ResourceKind, ExpressionFilter>,
    cache: DashMap<RID, bool>,
}

impl DtFilter {
    /// Empty resource rules select no objects.
    pub fn new(db_type: DbType) -> Self {
        let escape_pairs = SqlUtil::get_escape_pairs(&db_type);
        Self {
            resource_filters: HashMap::new(),
            db_type,
            matcher: PatternMatcher::new(escape_pairs),
            common_filters: HashMap::new(),
            expression_filters: HashMap::new(),
            cache: DashMap::new(),
        }
    }

    pub fn db_type(&self) -> &DbType {
        &self.db_type
    }

    pub fn resource_filters(&self) -> &HashMap<ResourceKind, ResourceFilter> {
        &self.resource_filters
    }

    pub fn filter_all_ddl(&self) -> bool {
        DdlType::iter().all(|ddl| self.filter_common(CommonFilterKind::Ddl, &ddl.to_string()))
    }

    pub fn filter_all_dcl(&self) -> bool {
        DclType::iter().all(|dcl| self.filter_dcl(&dcl))
    }

    pub fn excluded_columns<'a>(
        &self,
        table: impl Into<RID>,
        columns: impl IntoIterator<Item = &'a String>,
    ) -> Result<HashSet<String>> {
        let mut excluded = HashSet::new();
        let Some(rules) = self.resource_filters.get(&ResourceKind::Column) else {
            return Ok(excluded);
        };
        let table = table.into();
        let selected = ResourceFilter::matches_prefix(&rules.include_set, &table, &self.matcher);
        for column in columns {
            let id = table.child(column.clone());
            if ResourceFilter::matches(&rules.exclude_set, &id, &self.matcher)
                || (selected && !ResourceFilter::matches(&rules.include_set, &id, &self.matcher))
            {
                excluded.insert(column.clone());
            }
        }
        Ok(excluded)
    }

    pub fn set_common_filter(&mut self, kind: CommonFilterKind, filter: CommonFilter) {
        let previous = self.common_filters.entry(kind).or_default();
        previous.include_set.extend(filter.include_set);
        previous.exclude_set.extend(filter.exclude_set);
    }

    pub fn filter_common(&self, kind: CommonFilterKind, value: &str) -> bool {
        match self.common_filters.get(&kind) {
            Some(filter) => filter.excludes_value(value),
            None => kind.default_include() != WILDCARD,
        }
    }

    pub fn set_resource_filter(
        &mut self,
        kind: ResourceKind,
        filter: ResourceFilter,
    ) -> Result<()> {
        self.resource_filters.insert(kind, filter);
        self.cache.clear();
        Ok(())
    }

    pub fn add_include(&mut self, pattern: impl Into<RID>) -> Result<()> {
        let id: RID = pattern.into();
        let Some(kind) = ResourceKind::from_rid(&self.db_type, &id) else {
            return Ok(());
        };
        self.resource_filters
            .entry(kind)
            .or_default()
            .include_set
            .insert(id);
        self.cache.clear();
        Ok(())
    }

    pub fn add_exclude(&mut self, pattern: impl Into<RID>) -> Result<()> {
        let id: RID = pattern.into();
        let Some(kind) = ResourceKind::from_rid(&self.db_type, &id) else {
            return Ok(());
        };
        self.resource_filters
            .entry(kind)
            .or_default()
            .exclude_set
            .insert(id);
        self.cache.clear();
        Ok(())
    }

    pub fn filter(&self, id: impl Into<RID>) -> Result<bool> {
        let id = id.into();
        let kind = ResourceKind::from_rid(&self.db_type, &id);
        if kind == Some(ResourceKind::Column) {
            let table = id.prefix(id.len().saturating_sub(1));
            let Some(columns) = self.resource_filters.get(&ResourceKind::Column) else {
                return self.filter(table);
            };
            if self.matches_resource_rules(&id, true)
                || ResourceFilter::matches(&columns.exclude_set, &id, &self.matcher)
            {
                return Ok(true);
            }
            if ResourceFilter::matches(&columns.include_set, &id, &self.matcher) {
                return Ok(false);
            }
            if ResourceFilter::matches_prefix(&columns.include_set, &table, &self.matcher) {
                return Ok(true);
            }
            return self.filter(table);
        }
        let cacheable = matches!(kind, Some(ResourceKind::Table | ResourceKind::Collection));
        if cacheable {
            if let Some(value) = self.cache.get(&id) {
                return Ok(*value);
            }
        }
        let excluded =
            self.matches_resource_rules(&id, true) || !self.matches_resource_rules(&id, false);
        if cacheable {
            self.cache.insert(id, excluded);
        }
        Ok(excluded)
    }

    pub fn filter_event(&self, id: impl Into<RID>, event: &RowType) -> Result<bool> {
        let id = id.into();
        Ok(self.filter_common(CommonFilterKind::Event, &event.to_string()) || self.filter(id)?)
    }

    pub fn filter_ddl(&self, id: impl Into<RID>, ddl: &DdlType) -> Result<bool> {
        let id = id.into();
        Ok(self.filter_common(CommonFilterKind::Ddl, &ddl.to_string()) || self.filter(id)?)
    }

    pub fn filter_dcl(&self, dcl: &DclType) -> bool {
        self.filter_common(CommonFilterKind::Dcl, &dcl.to_string())
    }
    pub fn filter_structure(&self, kind: &StructureType) -> bool {
        self.filter_common(CommonFilterKind::Structure, &kind.to_string())
    }
    pub fn filter_cmd(&self, command: &str) -> bool {
        self.filter_common(CommonFilterKind::Cmd, command)
    }

    fn matches_resource_rules(&self, id: &RID, exclude: bool) -> bool {
        ResourceKind::levels(&self.db_type)
            .iter()
            .enumerate()
            .filter(|(_, kind)| **kind != ResourceKind::Column)
            .any(|(index, kind)| {
                let Some(filter) = self.resource_filters.get(kind) else {
                    return false;
                };
                let rules = if exclude {
                    &filter.exclude_set
                } else {
                    &filter.include_set
                };
                if index < id.len() {
                    ResourceFilter::matches(rules, &id.prefix(index + 1), &self.matcher)
                } else if exclude {
                    ResourceFilter::covers_descendants(rules, id, &self.matcher)
                } else {
                    // Descendant includes keep namespaces available for enumeration.
                    ResourceFilter::matches_prefix(rules, id, &self.matcher)
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use configparser::ini::Ini;

    use super::*;
    use crate::{config::filter_config::FilterConfig, rid};

    fn load_filter(ini: &Ini, db_type: DbType) -> Result<DtFilter> {
        DtFilter::from_config(&FilterConfig::from_ini(ini)?, db_type)
    }

    fn ini(text: &str) -> Ini {
        let mut ini = Ini::new();
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(text.into()).unwrap();
        ini
    }

    fn filter(text: &str) -> DtFilter {
        load_filter(&ini(text), DbType::Mysql).unwrap()
    }

    #[test]
    fn common_rules_are_added_on_demand_and_merge_without_duplicate_kinds() {
        let defaults = filter("");
        assert!(defaults.common_filters.is_empty());
        let mut f = filter("[source.event]\ninclude=insert\nexclude=delete");
        assert_eq!(f.common_filters.len(), 1);
        assert!(f.filter_common(CommonFilterKind::Event, "update"));
        let additional = CommonFilter::new(
            HashSet::from(["insert".into(), "update".into(), "delete".into()]),
            HashSet::from(["insert".into()]),
        );
        for _ in 0..2 {
            f.set_common_filter(CommonFilterKind::Event, additional.clone());
        }
        assert_eq!(f.common_filters.len(), 1);
        assert_eq!(
            f.common_filters[&CommonFilterKind::Event].include_set.len(),
            3
        );
        assert_eq!(
            f.common_filters[&CommonFilterKind::Event].exclude_set.len(),
            2
        );
        assert!(!f.filter_common(CommonFilterKind::Event, "update"));
        assert!(f.filter_common(CommonFilterKind::Event, "insert"));
        assert!(f.filter_common(CommonFilterKind::Event, "delete"));
        f.set_common_filter(
            CommonFilterKind::Cmd,
            CommonFilter::new(HashSet::from(["GET".into()]), HashSet::new()),
        );
        assert_eq!(f.common_filters.len(), 2);
        assert!(!f.filter_cmd("GET"));
        assert!(f.filter_cmd("SET"));
    }

    #[test]
    fn command_rules_are_literal_in_both_config_and_rule_api() {
        let names = [
            "GET",
            "SET*",
            "GET?",
            "r#GET|SET#",
            "r#GET{1,2}#",
            "r#[#",
            "`GET`",
            "FT.SEARCH",
        ];
        let rules = names.iter().map(|name| (*name).to_owned()).collect();
        let included = filter(&format!("[source.cmd]\ninclude={}", names.join(",")));
        let with_excludes = filter(&format!("[source.cmd]\nexclude={},*", names.join(",")));
        let mut direct = DtFilter::new(DbType::Mysql);
        direct.set_common_filter(
            CommonFilterKind::Cmd,
            CommonFilter::new(rules, HashSet::new()),
        );
        for name in names {
            assert!(!included.filter_cmd(name), "{name}");
            assert!(!direct.filter_cmd(name), "{name}");
            assert!(with_excludes.filter_cmd(name), "{name}");
        }
        for name in [
            "",
            "get",
            "SET",
            "SETEX",
            "GETX",
            "FT",
            "SEARCH",
            "line1\nline2",
        ] {
            assert!(included.filter_cmd(name), "{name}");
            assert!(direct.filter_cmd(name), "{name}");
            assert!(!with_excludes.filter_cmd(name), "{name}");
            assert!(!DtFilter::new(DbType::Mysql).filter_cmd(name), "{name}");
        }
        assert!(with_excludes.filter_cmd("*"));
    }

    #[test]
    fn namespace_discovery_and_projection_have_distinct_semantics() {
        let f = filter("[source.table]\ninclude=db1.t1,db1.t2\nexclude=db1.t2\n[source.column]\ninclude=db1.t1.id,db2.t1.id\nexclude=db1.t1.secret");
        assert!(!f.filter(rid!("db1")).unwrap());
        assert!(f.filter(rid!("db2")).unwrap());
        assert!(!f.filter(rid!("db1", "t1", "id")).unwrap());
        assert!(f.filter(rid!("db1", "t1", "other")).unwrap());
        assert!(f.filter(rid!("db1", "t1", "secret")).unwrap());
        assert!(f.filter(rid!("db1", "t2", "id")).unwrap());
        assert!(!f.filter(rid!("db2", "t1", "id")).unwrap());
        let columns = vec!["id".into(), "other".into(), "secret".into()];
        for (table, expected) in [
            (rid!("db1", "t1"), vec!["other", "secret"]),
            (rid!("db2", "t1"), vec!["other", "secret"]),
            (rid!("marker", "t"), vec![]),
        ] {
            assert_eq!(
                f.excluded_columns(&table, &columns).unwrap(),
                expected.into_iter().map(String::from).collect(),
            );
        }
        let f = filter("[source.db]\ninclude=db1\n[source.column]\ninclude=db1.t1.id");
        assert!(!f.filter(rid!("db1", "t2", "other")).unwrap());
        let f = filter("[source.db]\ninclude=db1\n[source.table]\nexclude=db1.*");
        assert!(f.filter(rid!("db1")).unwrap());
    }

    #[test]
    fn parent_excludes_apply_to_sparse_resource_filters() {
        let mut f = DtFilter::new(DbType::Mssql);
        let table = rid!("db", "sales", "order");
        let column = table.child("id");
        assert!(f.filter(&column).unwrap());
        f.add_include(&table).unwrap();
        for id in [
            rid!("db"),
            rid!("db", "sales"),
            table.clone(),
            column.clone(),
        ] {
            assert!(!f.filter(&id).unwrap(), "{id}");
        }
        assert!(f.filter(rid!("db", "other")).unwrap());
        assert!(f.filter(rid!("db", "sales", "other")).unwrap());
        f.add_exclude(&rid!("db")).unwrap();
        assert!(f.filter(&table).unwrap());
        assert!(f.filter(&column).unwrap());
        assert!(f.filter(rid!("db", "sales", "other")).unwrap());
        f.set_resource_filter(ResourceKind::Database, ResourceFilter::default())
            .unwrap();
        assert!(!f.filter(&column).unwrap());

        let mut columns_only = DtFilter::new(DbType::Mssql);
        columns_only.add_include(&column).unwrap();
        assert!(columns_only.filter(rid!("db")).unwrap());
        assert!(columns_only.filter(&table).unwrap());
        assert!(!columns_only.filter(&column).unwrap());
    }

    #[test]
    fn three_level_scope_and_cache_updates_keep_databases_isolated() {
        let mut f = load_filter(&ini("[source.schema]\ninclude=a.inventory,b.inventory\nexclude=a.audit\n[source.table]\nexclude=b.inventory.order"), DbType::Mssql).unwrap();
        assert!(!f.filter(rid!("a", "inventory", "order")).unwrap());
        assert!(f.filter(rid!("b", "inventory", "order")).unwrap());
        f.add_exclude(rid!("a")).unwrap();
        assert!(f.filter(rid!("a", "inventory", "order")).unwrap());
        assert!(f.filter(rid!("c")).unwrap());
        f.add_include(rid!("c", "dbo", "new")).unwrap();
        assert!(!f.filter(rid!("c")).unwrap());
        assert!(!f.filter(rid!("c", "dbo", "new")).unwrap());
    }

    #[test]
    fn added_rules_match_configuration_across_resource_layouts() {
        for (engine, rule, selected, excluded) in [
            (DbType::Mysql, "order*", "order_1", "preorder"),
            (DbType::Pg, "\"order*\"", "order*", "order1"),
            (DbType::Mssql, "[order*]", "order*", "order1"),
            (DbType::Mongo, "order?", "order", "order12"),
            (DbType::Redis, "r#order[0-9]+#", "order12", "order12tail"),
            (DbType::Kafka, "r#order*#", "orde", "order1"),
        ] {
            let levels = ResourceKind::levels(&engine);
            let depth = levels
                .iter()
                .take_while(|&&kind| kind != ResourceKind::Column)
                .count();
            let id = |name: &str| {
                let mut path = vec!["db"; depth - 1];
                path.push(name);
                RID::new(path)
            };
            let pattern = id(rule);
            let kind = levels[depth - 1];
            let mut f = DtFilter::new(engine.clone());
            assert!(f.filter(&id(selected)).unwrap());
            f.add_include(&pattern).unwrap();
            assert!(f.resource_filters[&kind].include_set.contains(&pattern));
            let configured = load_filter(
                &ini(&format!(
                    "[source.{}]\ninclude={}",
                    kind.as_ref(),
                    pattern.segments().join(".")
                )),
                engine.clone(),
            )
            .unwrap();
            for (name, excluded) in [(selected, false), (excluded, true)] {
                for filter in [&f, &configured] {
                    assert_eq!(
                        filter.filter(&id(name)).unwrap(),
                        excluded,
                        "{engine}: {rule} -> {name}"
                    );
                }
            }
            f.add_exclude(&pattern).unwrap();
            assert!(f.resource_filters[&kind].exclude_set.contains(&pattern));
            assert!(f.filter(&id(selected)).unwrap(), "{engine}: exclude {rule}");
        }
    }

    #[test]
    fn runtime_filtering_does_not_reject_resource_depths_or_kinds() {
        let f = filter("[source.table]\ninclude=db.orders\n[source.ddl]\ninclude=*");
        for (id, excluded) in [
            (rid!(""), false),
            (rid!("db"), false),
            (rid!("db", "orders"), false),
            (rid!("db", "orders", "id"), false),
            (rid!("db", "orders", "id", "extra"), false),
            (rid!("other", "orders", "id", "extra"), true),
        ] {
            assert_eq!(f.filter(&id).unwrap(), excluded, "{id}");
            assert_eq!(
                f.filter_event(&id, &RowType::Insert).unwrap(),
                excluded,
                "{id}"
            );
            assert_eq!(
                f.filter_ddl(&id, &DdlType::CreateTable).unwrap(),
                excluded,
                "{id}"
            );
        }
    }

    #[test]
    fn excludes_override_includes_across_resource_levels() {
        for engine in [DbType::Mysql, DbType::Pg, DbType::Mssql, DbType::Mongo] {
            let parent = if engine == DbType::Mssql {
                rid!("db", "sales")
            } else {
                rid!("sales")
            };
            let table = parent.child("orders");
            for (parent_exclude, child_include, child_exclude, excluded) in [
                (true, true, false, true),
                (false, false, true, true),
                (true, false, false, true),
                (false, false, false, false),
                (false, true, true, true),
            ] {
                let mut f = DtFilter::new(engine.clone());
                if parent_exclude {
                    f.add_exclude(&parent).unwrap();
                } else {
                    f.add_include(&parent).unwrap();
                }
                assert_eq!(f.filter(&table).unwrap(), parent_exclude);
                f.add_include(parent.child("events")).unwrap();
                if child_include {
                    f.add_include(&table).unwrap();
                }
                if child_exclude {
                    f.add_exclude(&table).unwrap();
                }
                for _ in 0..2 {
                    assert_eq!(f.filter(&table).unwrap(), excluded, "{engine}: {table}");
                }
            }
        }
    }

    #[test]
    fn legacy_and_basic_excludes_override_child_includes() {
        for (engine, kind) in [(DbType::Mysql, "database"), (DbType::Pg, "schema")] {
            for config in [
                "[filter]\ndo_dbs=kept\nignore_dbs=hidden*\ndo_tbs=hidden.orders\nignore_tbs=kept.secret".to_owned(),
                format!("[source.{kind}]\ninclude=kept\nexclude=hidden*\n[source.table]\ninclude=hidden.orders\nexclude=kept.secret\n[source.column]\ninclude=hidden.orders.id"),
            ] {
                let filter = load_filter(&ini(&config), engine.clone()).unwrap();
                for (id, excluded) in [
                    (rid!("hidden"), true), (rid!("hidden", "orders"), true),
                    (rid!("hidden", "orders", "id"), true),
                    (rid!("kept", "orders"), false), (rid!("kept", "secret"), true),
                ] {
                    assert_eq!(filter.filter(id).unwrap(), excluded, "{config}");
                }
            }
        }
    }
}

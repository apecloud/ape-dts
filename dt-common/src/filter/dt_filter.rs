use std::collections::HashMap;

use anyhow::{ensure, Context, Result};
use dashmap::DashMap;

use super::{CommonFilter, CommonFilterKind, ExpressionFilter, ResourceFilter};
use crate::{
    config::config_enums::DbType,
    meta::{
        dcl_meta::dcl_type::DclType, ddl_meta::ddl_type::DdlType, row_type::RowType,
        struct_meta::structure::structure_type::StructureType,
    },
    resource::{
        resource_depth, resource_kind, resource_levels, PatternMatcher, ResourceKind, ToRid, RID,
    },
    utils::sql_util::SqlUtil,
};

#[derive(Debug)]
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

    pub fn set_common_filter(&mut self, kind: CommonFilterKind, filter: CommonFilter) {
        let previous = self.common_filters.entry(kind).or_default();
        previous.do_set.extend(filter.do_set);
        previous.ignore_set.extend(filter.ignore_set);
    }

    pub fn filter_common(&self, kind: CommonFilterKind, value: &str) -> bool {
        match self.common_filters.get(&kind) {
            Some(filter) => filter.excludes_value(value),
            None => kind.default_do() != "*",
        }
    }

    pub fn set_resource_filter(
        &mut self,
        kind: ResourceKind,
        filter: ResourceFilter,
    ) -> Result<()> {
        let depth = resource_depth(&self.db_type, kind)
            .ok_or_else(|| anyhow::anyhow!("unsupported {kind:?} for {}", self.db_type))?;
        filter.validate(depth + 1, &self.matcher)?;
        self.resource_filters.insert(kind, filter);
        self.cache.clear();
        Ok(())
    }

    /// Preserve RID tokens for `PatternMatcher`, including quotes and regex delimiters.
    pub fn add_do<I: ToRid + ?Sized>(&mut self, pattern: &I) -> Result<()> {
        let id = pattern.to_rid(&self.db_type)?;
        let kind = resource_kind(&self.db_type, &id)?;
        self.matcher.validate(&id)?;
        self.resource_filters
            .entry(kind)
            .or_default()
            .do_set
            .insert(id);
        self.cache.clear();
        Ok(())
    }

    pub fn add_ignore<I: ToRid + ?Sized>(&mut self, pattern: &I) -> Result<()> {
        let id = pattern.to_rid(&self.db_type)?;
        let kind = resource_kind(&self.db_type, &id)?;
        self.matcher.validate(&id)?;
        self.resource_filters
            .entry(kind)
            .or_default()
            .ignore_set
            .insert(id);
        self.cache.clear();
        Ok(())
    }

    pub fn filter<I: ToRid + ?Sized>(&self, resource: &I) -> Result<bool> {
        self.filter_rid(&resource.to_rid(&self.db_type)?)
    }

    pub fn filter_rid(&self, id: &RID) -> Result<bool> {
        let kind = resource_kind(&self.db_type, id)?;
        if kind == ResourceKind::Column {
            let table = id
                .parent()
                .with_context(|| format!("column resource has no parent: {id}"))?;
            let Some(columns) = self.resource_filters.get(&kind) else {
                return self.filter_rid(&table);
            };
            return Ok(self.filter_rid(&table)?
                || ResourceFilter::matches(&columns.ignore_set, id, &self.matcher)
                || (ResourceFilter::matches_prefix(&columns.do_set, &table, &self.matcher)
                    && !ResourceFilter::matches(&columns.do_set, id, &self.matcher)));
        }
        let cacheable = matches!(kind, ResourceKind::Table | ResourceKind::Collection);
        if cacheable {
            if let Some(value) = self.cache.get(id) {
                return Ok(*value);
            }
        }
        let mut allowed = false;
        let mut ignored = false;
        for (index, kind) in resource_levels(&self.db_type).iter().enumerate().rev() {
            if *kind == ResourceKind::Column {
                continue;
            }
            let Some(base) = self.resource_filters.get(kind) else {
                continue;
            };
            if index < id.len() {
                let depth = index + 1;
                let prefix = id
                    .prefix(depth)
                    .with_context(|| format!("invalid filter prefix depth {depth}: {id}"))?;
                ignored |= ResourceFilter::matches(&base.ignore_set, &prefix, &self.matcher);
                allowed |= ResourceFilter::matches(&base.do_set, &prefix, &self.matcher);
            } else {
                // Descendant includes keep namespaces available for enumeration.
                allowed |= ResourceFilter::matches_prefix(&base.do_set, id, &self.matcher);
                ignored |= ResourceFilter::covers_descendants(&base.ignore_set, id, &self.matcher);
            }
        }
        let excluded = ignored || !allowed;
        if cacheable {
            self.cache.insert(id.clone(), excluded);
        }
        Ok(excluded)
    }

    pub fn filter_event<I: ToRid + ?Sized>(&self, resource: &I, event: &RowType) -> Result<bool> {
        let id = resource.to_rid(&self.db_type)?;
        let kind = resource_kind(&self.db_type, &id)?;
        ensure!(
            matches!(
                kind,
                ResourceKind::Table
                    | ResourceKind::Collection
                    | ResourceKind::Topic
                    | ResourceKind::Database
            ) && id.len()
                == resource_levels(&self.db_type)
                    .iter()
                    .take_while(|&&k| k != ResourceKind::Column)
                    .count(),
            "event filtering requires a data object"
        );
        Ok(
            self.filter_common(CommonFilterKind::Event, &event.to_string())
                || self.filter_rid(&id)?,
        )
    }

    pub fn filter_ddl<I: ToRid + ?Sized>(&self, resource: &I, ddl: &DdlType) -> Result<bool> {
        let id = resource.to_rid(&self.db_type)?;
        resource_kind(&self.db_type, &id)?;
        Ok(self.filter_common(CommonFilterKind::Ddl, &ddl.to_string()) || self.filter_rid(&id)?)
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
        let mut f = filter("[rules.event]\ndo=insert\nignore=delete");
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
        assert_eq!(f.common_filters[&CommonFilterKind::Event].do_set.len(), 3);
        assert_eq!(
            f.common_filters[&CommonFilterKind::Event].ignore_set.len(),
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
        let included = filter(&format!("[rules.cmd]\ndo={}", names.join(",")));
        let ignored = filter(&format!("[rules.cmd]\nignore={},*", names.join(",")));
        let mut direct = DtFilter::new(DbType::Mysql);
        direct.set_common_filter(
            CommonFilterKind::Cmd,
            CommonFilter::new(rules, HashSet::new()),
        );
        for name in names {
            assert!(!included.filter_cmd(name), "{name}");
            assert!(!direct.filter_cmd(name), "{name}");
            assert!(ignored.filter_cmd(name), "{name}");
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
            assert!(!ignored.filter_cmd(name), "{name}");
            assert!(!DtFilter::new(DbType::Mysql).filter_cmd(name), "{name}");
        }
        assert!(ignored.filter_cmd("*"));
    }

    #[test]
    fn namespace_discovery_and_projection_have_distinct_semantics() {
        let f = filter("[rules.table]\ndo=db1.t1,db1.t2\nignore=db1.t2\n[rules.column]\ndo=db1.t1.id,db2.t1.id\nignore=db1.t1.secret");
        assert!(!f.filter(&rid!("db1")).unwrap());
        assert!(f.filter(&rid!("db2")).unwrap());
        assert!(!f.filter(&rid!("db1", "t1", "id")).unwrap());
        assert!(f.filter(&rid!("db1", "t1", "other")).unwrap());
        assert!(f.filter(&rid!("db1", "t1", "secret")).unwrap());
        assert!(f.filter(&rid!("db1", "t2", "id")).unwrap());
        assert!(f.filter(&rid!("db2", "t1", "id")).unwrap());
        let f = filter("[rules.db]\ndo=db1\n[rules.column]\ndo=db1.t1.id");
        assert!(!f.filter(&rid!("db1", "t2", "other")).unwrap());
        let f = filter("[rules.db]\ndo=db1\n[rules.table]\nignore=db1.*");
        assert!(f.filter(&rid!("db1")).unwrap());
    }

    #[test]
    fn sparse_resource_filters_keep_namespace_discovery_and_parent_exclusions() {
        let mut f = DtFilter::new(DbType::Mssql);
        let table = rid!("db", "sales", "order");
        let column = table.child("id").unwrap();
        assert!(f.filter(&column).unwrap());
        f.add_do(&table).unwrap();
        for id in [
            rid!("db"),
            rid!("db", "sales"),
            table.clone(),
            column.clone(),
        ] {
            assert!(!f.filter(&id).unwrap(), "{id}");
        }
        assert!(f.filter(&rid!("db", "other")).unwrap());
        assert!(f.filter(&rid!("db", "sales", "other")).unwrap());
        f.add_ignore(&rid!("db")).unwrap();
        assert!(f.filter(&table).unwrap());
        assert!(f.filter(&column).unwrap());
        f.set_resource_filter(ResourceKind::Database, ResourceFilter::default())
            .unwrap();
        assert!(!f.filter(&column).unwrap());

        let mut columns_only = DtFilter::new(DbType::Mssql);
        columns_only.add_do(&column).unwrap();
        assert!(columns_only.filter(&rid!("db")).unwrap());
        assert!(columns_only.filter(&table).unwrap());
        assert!(columns_only.filter(&column).unwrap());
    }

    #[test]
    fn three_level_scope_and_cache_updates_keep_databases_isolated() {
        let mut f = load_filter(&ini("[rules.schema]\ndo=a.inventory,b.inventory\nignore=a.audit\n[rules.table]\nignore=b.inventory.order"), DbType::Mssql).unwrap();
        assert!(!f.filter(&rid!("a", "inventory", "order")).unwrap());
        assert!(f.filter(&rid!("b", "inventory", "order")).unwrap());
        f.add_ignore(&rid!("a")).unwrap();
        assert!(f.filter(&rid!("a", "inventory", "order")).unwrap());
        assert!(f.filter(&rid!("c")).unwrap());
        f.add_do(&rid!("c", "dbo", "new")).unwrap();
        assert!(!f.filter(&rid!("c")).unwrap());
        assert!(!f.filter(&rid!("c", "dbo", "new")).unwrap());
    }

    #[test]
    fn resource_filters_accept_rid_sets_and_validate_resource_depths() {
        let mut f = DtFilter::new(DbType::Mysql);
        f.set_resource_filter(
            ResourceKind::Table,
            ResourceFilter::new(
                HashSet::from([rid!("db", "order*"), rid!("db", "`*`")]),
                HashSet::from([rid!("db", "order_secret")]),
            ),
        )
        .unwrap();
        assert!(!f.filter(&rid!("db")).unwrap());
        assert!(!f.filter(&rid!("db", "order10")).unwrap());
        assert!(!f.filter(&rid!("db", "*")).unwrap());
        assert!(f.filter(&rid!("db", "other")).unwrap());
        assert!(f.filter(&rid!("db", "order_secret")).unwrap());
        for invalid in [
            ResourceFilter::new(HashSet::from([rid!("db")]), HashSet::new()),
            ResourceFilter::new(HashSet::new(), HashSet::from([rid!("db")])),
            ResourceFilter::new(HashSet::from([rid!("db", "r#[#")]), HashSet::new()),
            ResourceFilter::new(HashSet::new(), HashSet::from([rid!("db", "r#[#")])),
        ] {
            assert!(f.set_resource_filter(ResourceKind::Table, invalid).is_err());
        }
        assert!(!f.filter(&rid!("db", "order10")).unwrap());
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
            let levels = resource_levels(&engine);
            let depth = levels
                .iter()
                .take_while(|&&kind| kind != ResourceKind::Column)
                .count();
            let id = |name: &str| {
                let mut path = vec!["db"; depth - 1];
                path.push(name);
                RID::new(path).unwrap()
            };
            let pattern = id(rule);
            let kind = levels[depth - 1];
            let mut f = DtFilter::new(engine.clone());
            assert!(f.filter(&id(selected)).unwrap()); // Prime the cache.
            f.add_do(&pattern).unwrap();
            assert!(f.resource_filters[&kind].do_set.contains(&pattern));
            let configured = load_filter(
                &ini(&format!(
                    "[rules.{}]\ndo={}",
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
            f.add_ignore(&pattern).unwrap();
            assert!(f.resource_filters[&kind].ignore_set.contains(&pattern));
            assert!(f.filter(&id(selected)).unwrap(), "{engine}: ignore {rule}");
        }
    }

    #[test]
    fn invalid_added_patterns_leave_existing_rules_and_cache_intact() {
        let mut f = DtFilter::new(DbType::Mysql);
        f.add_do(&rid!("db", "order*")).unwrap();
        let table = rid!("db", "orders");
        assert!(!f.filter(&table).unwrap());
        for pattern in [rid!("db", "r#[#"), rid!("db", "orders", "id", "extra")] {
            assert!(f.add_do(&pattern).is_err());
            assert!(f.add_ignore(&pattern).is_err());
            assert!(!f.filter(&table).unwrap());
        }
        assert_eq!(f.resource_filters[&ResourceKind::Table].do_set.len(), 1);
        assert!(f.resource_filters[&ResourceKind::Table]
            .ignore_set
            .is_empty());
    }

    #[test]
    fn resource_sections_use_raw_regexes_with_legacy_matcher_semantics() {
        for section in ["[filter]\ndo_dbs", "[rules.db]\ndo"] {
            let f = filter(&format!("{section}=r#inventory|market#"));
            assert!(f.resource_filters[&ResourceKind::Database]
                .do_set
                .contains(&rid!("r#inventory|market#")));
            for (name, excluded) in [
                ("inventory", false),
                ("inventory_extra", false),
                ("market", false),
                ("othermarket", false),
                ("unselected", true),
            ] {
                assert_eq!(
                    f.filter(&RID::new([name]).unwrap()).unwrap(),
                    excluded,
                    "{section} {name}"
                );
            }
        }
    }
}

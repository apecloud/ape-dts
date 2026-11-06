use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use dashmap::DashMap;
use strum::IntoEnumIterator;

use super::{CommonFilter, CommonFilterKind, ResourceFilter};
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
    pub where_conditions: HashMap<RID, String>,
    cache: DashMap<RID, bool>,
}

impl DtFilter {
    pub fn new(db_type: DbType) -> Self {
        let escape_pairs = SqlUtil::get_escape_pairs(&db_type);
        Self {
            resource_filters: HashMap::new(),
            db_type,
            matcher: PatternMatcher::new(escape_pairs),
            common_filters: HashMap::new(),
            where_conditions: HashMap::new(),
            cache: DashMap::new(),
        }
    }

    pub fn db_type(&self) -> &DbType {
        &self.db_type
    }

    pub fn resource_filters(&self) -> &HashMap<ResourceKind, ResourceFilter> {
        &self.resource_filters
    }

    pub fn where_condition(&self, id: impl Into<RID>) -> Result<Option<&str>> {
        let id = id.into();
        if ResourceKind::from_rid(&self.db_type, &id).is_none() {
            return Ok(None);
        }
        Ok(self.where_conditions.get(&id).map(String::as_str))
    }

    pub fn set_where_condition(&mut self, id: RID, condition: String) -> Result<()> {
        if ResourceKind::from_rid(&self.db_type, &id).is_some() {
            self.where_conditions.entry(id).or_insert(condition);
        }
        Ok(())
    }

    pub(super) fn set_where_conditions(&mut self, conditions: HashMap<RID, String>) {
        for (id, condition) in conditions {
            self.where_conditions.entry(id).or_insert(condition);
        }
    }

    pub fn filter_all_ddl(&self) -> bool {
        DdlType::iter().all(|ddl| self.filter_common(CommonFilterKind::Ddl, &ddl.to_string()))
    }

    pub fn filter_all_dcl(&self) -> bool {
        DclType::iter().all(|dcl| self.filter_dcl(&dcl))
    }

    pub fn excluded_children<'a>(
        &self,
        parent: impl Into<RID>,
        children: impl IntoIterator<Item = &'a String>,
    ) -> Result<HashSet<String>> {
        self.excluded_children_with_scope(parent.into(), children, false)
    }

    pub fn excluded_children_in_scope<'a>(
        &self,
        parent: impl Into<RID>,
        children: impl IntoIterator<Item = &'a String>,
    ) -> Result<HashSet<String>> {
        self.excluded_children_with_scope(parent.into(), children, true)
    }

    fn excluded_children_with_scope<'a>(
        &self,
        parent: RID,
        children: impl IntoIterator<Item = &'a String>,
        selected_parent: bool,
    ) -> Result<HashSet<String>> {
        ResourceKind::from_rid(&self.db_type, &parent)
            .and_then(|kind| kind.child(&self.db_type))
            .with_context(|| format!("resource has no registered children: {parent}"))?;
        let has_descendant_rules = ResourceKind::levels(&self.db_type)
            .iter()
            .skip(parent.len())
            .any(|kind| {
                self.resource_filters.get(kind).is_some_and(|rules| {
                    !rules.include_set.is_empty() || !rules.exclude_set.is_empty()
                })
            });
        if !has_descendant_rules {
            let filtered = if selected_parent {
                self.matches_exclude(&parent)
            } else {
                self.filter(&parent)?
            };
            return Ok(if filtered {
                children.into_iter().cloned().collect()
            } else {
                HashSet::new()
            });
        }
        let mut excluded = HashSet::new();
        for child in children {
            let id = parent.child(child.clone());
            let filtered = if selected_parent {
                self.matches_exclude(&id) || !self.matches_include_from(&id, parent.len(), true)
            } else {
                self.filter(id)?
            };
            if filtered {
                excluded.insert(child.clone());
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
        let kind = ResourceKind::from_rid(&self.db_type, &id)
            .ok_or_else(|| anyhow::anyhow!("invalid resource path for {}: {}", self.db_type, id))?;

        if kind.is_cacheable() {
            if let Some(value) = self.cache.get(&id) {
                return Ok(*value);
            }
        }
        let excluded = self.matches_exclude(&id) || !self.matches_include(&id);
        if kind.is_cacheable() {
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

    fn matches_exclude(&self, id: &RID) -> bool {
        ResourceKind::levels(&self.db_type)
            .iter()
            .enumerate()
            .any(|(index, kind)| {
                let Some(filter) = self.resource_filters.get(kind) else {
                    return false;
                };
                let rules = &filter.exclude_set;
                if index < id.len() {
                    ResourceFilter::matches(rules, &id.prefix(index + 1), &self.matcher)
                } else {
                    ResourceFilter::covers_descendants(rules, id, &self.matcher)
                }
            })
    }

    fn matches_include(&self, id: &RID) -> bool {
        self.matches_include_from(id, 0, false)
    }

    fn matches_include_from(&self, id: &RID, start: usize, mut inherited: bool) -> bool {
        let levels = ResourceKind::levels(&self.db_type);
        for (index, kind) in levels.iter().take(id.len()).enumerate().skip(start) {
            let Some(rules) = self.resource_filters.get(kind) else {
                continue;
            };
            if ResourceFilter::matches_prefix(&rules.include_set, &id.prefix(index), &self.matcher)
            {
                inherited = ResourceFilter::matches(
                    &rules.include_set,
                    &id.prefix(index + 1),
                    &self.matcher,
                );
            }
        }
        inherited
            || levels.iter().skip(id.len()).any(|kind| {
                self.resource_filters.get(kind).is_some_and(|rules| {
                    ResourceFilter::matches_prefix(&rules.include_set, id, &self.matcher)
                })
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
    fn where_ids_are_literal() {
        let mut filter = DtFilter::new(DbType::Mysql);
        for name in ["order*", "event?", "r#[#", "a.b,c:d"] {
            let id = rid!("db", name);
            filter.set_where_condition(id.clone(), name.into()).unwrap();
            assert_eq!(filter.where_condition(id).unwrap(), Some(name));
        }
        for id in [
            rid!("db"),
            rid!("db", "orders"),
            rid!("db", "event1"),
            rid!("db", "order*", "id"),
        ] {
            assert_eq!(filter.where_condition(id).unwrap(), None);
        }
    }

    #[test]
    fn where_updates_keep_first_conditions_and_ignore_unsupported_depths() {
        let mut filter = DtFilter::new(DbType::Mysql);
        let table = rid!("db", "t");
        for condition in ["id > 1", "id > 1", "id > 2"] {
            filter
                .set_where_condition(table.clone(), condition.into())
                .unwrap();
        }
        let invalid = rid!("db", "t", "id", "extra");
        filter
            .set_where_condition(invalid.clone(), "unused".into())
            .unwrap();
        filter
            .set_where_condition(rid!("db"), "db condition".into())
            .unwrap();
        filter.set_where_conditions(HashMap::from([
            (table.clone(), "later".into()),
            (rid!("db", "other"), "other".into()),
            (rid!("db"), "different level".into()),
        ]));
        assert_eq!(filter.where_condition(&table).unwrap(), Some("id > 1"));
        assert_eq!(filter.where_condition(invalid).unwrap(), None);
        assert_eq!(
            filter.where_condition(rid!("db", "other")).unwrap(),
            Some("other")
        );
        assert_eq!(
            filter.where_condition(rid!("db")).unwrap(),
            Some("db condition")
        );
        assert_eq!(filter.where_conditions.len(), 3);
    }

    #[test]
    fn common_rules_are_added_on_demand_and_merge_without_duplicate_kinds() {
        let defaults = filter("");
        assert!(defaults.common_filters.is_empty());
        let mut f = filter("[filter]\ndo_events=insert");
        f.set_common_filter(
            CommonFilterKind::Event,
            CommonFilter::new(HashSet::new(), HashSet::from(["delete".into()])),
        );
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
    fn command_rules_are_literal_in_rule_api() {
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
        let rules: HashSet<String> = names.iter().map(|name| (*name).to_owned()).collect();
        let mut included = DtFilter::new(DbType::Mysql);
        included.set_common_filter(
            CommonFilterKind::Cmd,
            CommonFilter::new(rules.clone(), HashSet::new()),
        );
        let mut excludes = rules.clone();
        excludes.insert("*".into());
        let mut with_excludes = DtFilter::new(DbType::Mysql);
        with_excludes.set_common_filter(
            CommonFilterKind::Cmd,
            CommonFilter::new(HashSet::from(["*".into()]), excludes),
        );
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
    fn includes_constrain_only_their_parent_scope_and_excludes_always_win() {
        for (config, checks) in [
            ("[source.column]\ninclude=db1.tb1.id", vec![
                (rid!("db1"), false), (rid!("db1", "tb1"), false),
                (rid!("db1", "tb1", "id"), false), (rid!("db1", "tb1", "other"), true),
                (rid!("db1", "tb2"), true), (rid!("db2"), true),
            ]),
            ("[source.table]\ninclude=db1.tb1,db1.tb2\n[source.column]\ninclude=db1.tb1.id,db1.tb1.name", vec![
                (rid!("db1", "tb1", "id"), false), (rid!("db1", "tb1", "name"), false),
                (rid!("db1", "tb1", "secret"), true), (rid!("db1", "tb2", "secret"), false),
                (rid!("db1", "tb3"), true),
            ]),
            ("[source.db]\ninclude=db1,db2\n[source.table]\ninclude=db1.tb1\n[source.column]\ninclude=db1.tb1.id", vec![
                (rid!("db1", "tb1", "id"), false), (rid!("db1", "tb1", "other"), true),
                (rid!("db1", "tb2"), true), (rid!("db2", "tb2", "other"), false),
            ]),
            ("[source.db]\ninclude=db1\n[source.column]\ninclude=db2.tb1.id", vec![
                (rid!("db2"), false), (rid!("db2", "tb1"), false), (rid!("db2", "tb1", "id"), false),
                (rid!("db2", "tb1", "other"), true), (rid!("db2", "tb2"), true),
            ]),
            ("[source.table]\ninclude=db1.tb1\n[source.column]\ninclude=db1.tb2.id", vec![
                (rid!("db1", "tb2"), false), (rid!("db1", "tb2", "id"), false),
                (rid!("db1", "tb2", "other"), true), (rid!("db1", "tb1", "other"), false),
            ]),
            ("[source.db]\ninclude=db1\n[source.column]\nexclude=db1.tb1.*", vec![
                (rid!("db1"), false), (rid!("db1", "tb1"), true),
                (rid!("db1", "tb1", "id"), true), (rid!("db1", "tb2", "id"), false),
            ]),
            ("[source.table]\ninclude=db1.tb1\n[source.column]\nexclude=db1.tb1.`*`", vec![
                (rid!("db1", "tb1"), false), (rid!("db1", "tb1", "id"), false),
                (rid!("db1", "tb1", "*"), true),
            ]),
            ("[source.db]\nexclude=db1\n[source.column]\ninclude=db1.tb1.id", vec![
                (rid!("db1"), true), (rid!("db1", "tb1"), true), (rid!("db1", "tb1", "id"), true),
            ]),
            ("[source.table]\ninclude=db1.tb1\n[source.column]\ninclude=db1.tb1.id\nexclude=db1.tb1.id", vec![
                (rid!("db1", "tb1"), false), (rid!("db1", "tb1", "id"), true),
                (rid!("db1", "tb1", "other"), true),
            ]),
            ("[source.table]\ninclude=db1.tb1\n[source.column]\ninclude=", vec![
                (rid!("db1", "tb1", "other"), false),
            ]),
        ] {
            let f = filter(config);
            for (id, excluded) in checks {
                assert_eq!(f.filter(&id).unwrap(), excluded, "{config}: {id}");
            }
        }
        let f = filter("[source.table]\ninclude=db1.tb1,db1.tb2\n[source.column]\ninclude=db1.tb1.id\nexclude=db1.tb2.secret");
        let columns = vec!["id".into(), "other".into(), "secret".into()];
        for (table, expected) in [
            (rid!("db1", "tb1"), vec!["other", "secret"]),
            (rid!("db1", "tb2"), vec!["secret"]),
            (rid!("db2", "tb1"), vec!["id", "other", "secret"]),
        ] {
            assert_eq!(
                f.excluded_children(table, &columns).unwrap(),
                expected.into_iter().map(String::from).collect()
            );
        }
    }

    #[test]
    fn children_without_descendant_rules_inherit_selection_without_iteration() {
        let columns = ["id".into(), "name".into()];
        for config in [
            "[source.db]\ninclude=db",
            "[source.table]\ninclude=db.tb\n[source.column]\ninclude=\nexclude=",
            "[filter]\ndo_dbs=db\ndo_tbs=db.tb",
        ] {
            let f = filter(config);
            let visited = std::cell::Cell::new(0);
            let children = columns.iter().inspect(|_| visited.set(visited.get() + 1));
            assert!(f
                .excluded_children(rid!("db", "tb"), children)
                .unwrap()
                .is_empty());
            assert_eq!(visited.get(), 0, "{config}");
        }

        let f = filter("[source.table]\ninclude=db.selected");
        let parent = rid!("db", "logged");
        assert!(f.filter(&parent).unwrap());
        let visited = std::cell::Cell::new(0);
        let children = columns.iter().inspect(|_| visited.set(visited.get() + 1));
        assert!(f
            .excluded_children_in_scope(&parent, children)
            .unwrap()
            .is_empty());
        assert_eq!(visited.get(), 0);
        assert!(f.filter(&parent).unwrap());
    }

    #[test]
    fn inherited_children_preserve_exclusions_and_deeper_rules() {
        let children = ["keep".into(), "drop".into()];
        for (config, parent, scoped, expected) in [
            (
                "[source.table]\ninclude=db.other",
                rid!("db", "tb"),
                false,
                vec!["keep", "drop"],
            ),
            (
                "[source.db]\ninclude=db\nexclude=db",
                rid!("db", "tb"),
                false,
                vec!["keep", "drop"],
            ),
            (
                "[source.table]\ninclude=db.other\nexclude=db.tb",
                rid!("db", "tb"),
                true,
                vec!["keep", "drop"],
            ),
            (
                "[source.column]\ninclude=db.keep.id",
                rid!("db"),
                false,
                vec!["drop"],
            ),
            (
                "[source.db]\ninclude=db\n[source.column]\nexclude=db.drop.*",
                rid!("db"),
                false,
                vec!["drop"],
            ),
        ] {
            let f = filter(config);
            let actual = if scoped {
                f.excluded_children_in_scope(parent, &children)
            } else {
                f.excluded_children(parent, &children)
            };
            assert_eq!(
                actual.unwrap(),
                expected.into_iter().map(String::from).collect(),
                "{config}"
            );
        }
    }

    #[test]
    fn explicit_descendant_includes_select_paths_outside_ancestor_whitelists() {
        let mut f =
            filter("[source.db]\ninclude=inventory\n[source.table]\ninclude=archive.orders");
        for (id, excluded) in [
            (rid!("inventory"), false),
            (rid!("inventory", "any_table"), false),
            (rid!("inventory", "any_table", "any_column"), false),
            (rid!("archive"), false),
            (rid!("archive", "orders"), false),
            (rid!("archive", "orders", "any_column"), false),
            (rid!("archive", "other"), true),
            (rid!("unselected"), true),
        ] {
            assert_eq!(f.filter(&id).unwrap(), excluded, "{id}");
        }
        f.add_include(rid!("archive", "other", "id")).unwrap();
        assert!(!f.filter(rid!("archive", "other")).unwrap());
        assert!(!f.filter(rid!("archive", "other", "id")).unwrap());
        assert!(f.filter(rid!("archive", "other", "name")).unwrap());
        f.add_exclude(rid!("archive")).unwrap();
        assert!(f.filter(rid!("archive", "orders")).unwrap());
        assert!(f.filter(rid!("archive", "other", "id")).unwrap());
        assert!(!f.filter(rid!("inventory", "any_table")).unwrap());
    }

    #[test]
    fn scoped_inheritance_and_leaf_discovery_work_at_every_registered_level() {
        for engine in [
            DbType::Mysql,
            DbType::Pg,
            DbType::Mssql,
            DbType::Mongo,
            DbType::Redis,
            DbType::Kafka,
        ] {
            let levels = ResourceKind::levels(&engine);
            let leaf = RID::new(vec!["selected"; levels.len()]);
            let mut f = DtFilter::new(engine.clone());
            f.add_include(&leaf).unwrap();
            for depth in 1..=leaf.len() {
                assert!(!f.filter(leaf.prefix(depth)).unwrap(), "{engine}: {depth}");
                assert!(f.filter(leaf.prefix(depth - 1).child("sibling")).unwrap());
            }
            f.add_exclude(leaf.prefix(1)).unwrap();
            for depth in 1..=leaf.len() {
                assert!(f.filter(leaf.prefix(depth)).unwrap());
            }
            let mut discovered = DtFilter::new(engine.clone());
            discovered.add_include(rid!("another_root")).unwrap();
            discovered.add_include(&leaf).unwrap();
            for depth in 1..=leaf.len() {
                assert!(!discovered.filter(leaf.prefix(depth)).unwrap());
                assert!(discovered
                    .filter(leaf.prefix(depth - 1).child("unselected"))
                    .unwrap());
            }
            for parent_depth in 1..levels.len() {
                let prefix = RID::new(vec!["scope"; parent_depth - 1]);
                let first = prefix.child("first");
                let second = prefix.child("second");
                let mut f = DtFilter::new(engine.clone());
                f.add_include(&first).unwrap();
                f.add_include(&second).unwrap();
                f.add_include(first.child("keep")).unwrap();
                f.add_include(first.child("also_keep")).unwrap();
                assert!(!f.filter(first.child("keep")).unwrap());
                assert!(!f.filter(first.child("also_keep")).unwrap());
                assert!(f.filter(first.child("drop")).unwrap());
                assert!(!f.filter(second.child("drop")).unwrap());
                let children = vec!["keep".into(), "drop".into()];
                assert_eq!(
                    f.excluded_children(&first, &children).unwrap(),
                    HashSet::from(["drop".into()])
                );
                f.add_exclude(first.child("*")).unwrap();
                assert!(f.filter(&first).unwrap());
                assert!(f.filter(first.child("keep")).unwrap());
                assert!(!f.filter(&second).unwrap());
            }
            assert!(DtFilter::new(engine)
                .excluded_children(leaf, &Vec::<String>::new())
                .is_err());
        }
    }

    #[test]
    fn wildcard_parent_includes_constrain_the_matching_parent_scopes() {
        let mut f = filter("[source.db]\ninclude=inventory,scope_*\n[source.table]\ninclude=*.rules_shared\n[source.column]\ninclude=inventory.orders.id");
        for (id, excluded) in [
            (rid!("inventory", "fallback"), true),
            (rid!("inventory", "rules_shared"), false),
            (rid!("inventory", "orders", "id"), false),
            (rid!("inventory", "orders", "amount"), true),
            (rid!("scope_eu", "orders"), true),
            (rid!("scope_eu", "rules_shared"), false),
            (rid!("other", "rules_shared"), false),
            (rid!("other", "fallback"), true),
        ] {
            assert_eq!(f.filter(&id).unwrap(), excluded, "{id}");
        }
        f.add_include(rid!("inventory", "fallback")).unwrap();
        f.add_include(rid!("scope_*", "orders")).unwrap();
        assert!(!f.filter(rid!("inventory", "fallback")).unwrap());
        assert!(!f.filter(rid!("inventory", "fallback", "amount")).unwrap());
        assert!(!f.filter(rid!("scope_eu", "orders")).unwrap());
        f.add_exclude(rid!("inventory")).unwrap();
        assert!(f.filter(rid!("inventory", "fallback")).unwrap());
        assert!(f.filter(rid!("inventory", "orders", "id")).unwrap());
    }

    #[test]
    fn wildcard_parent_scopes_and_log_selected_parents_keep_local_rules() {
        let f = filter("[source.db]\ninclude=db\n[source.column]\ninclude=db.tb*.id,db.r#event[0-9]+#.name,db.`literal*`.id");
        for (table, column, excluded) in [
            ("tb1", "id", false),
            ("tb1", "other", true),
            ("tb2", "other", true),
            ("event12", "name", false),
            ("event12", "id", true),
            ("other", "other", false),
            ("literal*", "other", true),
            ("literal1", "other", false),
        ] {
            assert_eq!(f.filter(rid!("db", table, column)).unwrap(), excluded);
        }
        let f = filter(
            "[source.table]\ninclude=db.selected\n[source.column]\nexclude=db.logged.secret",
        );
        let columns = vec!["id".into(), "other".into(), "secret".into()];
        let logged = rid!("db", "logged");
        assert!(f.filter(&logged).unwrap());
        assert_eq!(
            f.excluded_children_in_scope(&logged, &columns).unwrap(),
            HashSet::from(["secret".into()])
        );
        // Replay scope is local and must not broaden the original filter.
        assert!(f.filter(&logged).unwrap());
        let f =
            filter("[source.table]\ninclude=db.selected\n[source.column]\ninclude=db.logged.id");
        assert_eq!(
            f.excluded_children_in_scope(&logged, &columns).unwrap(),
            HashSet::from(["other".into(), "secret".into()])
        );
        let f = filter("[source.table]\nexclude=db.logged\n[source.column]\ninclude=db.logged.id");
        assert_eq!(
            f.excluded_children_in_scope(&logged, &columns).unwrap(),
            columns.into_iter().collect()
        );
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
        assert!(!columns_only.filter(rid!("db")).unwrap());
        assert!(!columns_only.filter(&table).unwrap());
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
    fn runtime_filtering_rejects_unregistered_resource_paths() {
        let f = filter("[source.table]\ninclude=db.orders\n[filter]\ndo_ddls=*");
        for (id, excluded) in [
            (rid!("db"), false),
            (rid!("db", "orders"), false),
            (rid!("db", "orders", "id"), false),
            (rid!("db", "other"), true),
            (rid!("other", "orders", "id"), true),
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
        for id in [rid!(""), rid!("db", "orders", "id", "extra")] {
            assert!(f.filter(&id).is_err());
            assert!(f.filter_event(&id, &RowType::Insert).is_err());
            assert!(f.filter_ddl(&id, &DdlType::CreateTable).is_err());
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
                (false, false, false, true),
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

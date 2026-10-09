//! Standalone resource filtering. `true` means excluded, as in the existing RdbFilter.
mod config;
mod pattern;

use std::collections::{HashMap, HashSet};

use anyhow::{ensure, Result};
use dashmap::DashMap;
pub use pattern::IdPattern;
use pattern::MatcherSet;

pub use crate::resource::config::BASE_FILTER_SECTIONS;
use crate::{
    config::config_enums::DbType,
    meta::{
        dcl_meta::dcl_type::DclType, ddl_meta::ddl_type::DdlType, row_type::RowType,
        struct_meta::structure::structure_type::StructureType,
    },
    resource::{
        resource_depth, resource_kind, resource_levels, validate_rid, ResourceKind, ToRid, RID,
    },
};

#[derive(Debug, Clone, Default)]
pub struct BaseFilter {
    do_set: MatcherSet,
    ignore_set: MatcherSet,
}

impl BaseFilter {
    pub fn new(do_rules: Vec<IdPattern>, ignore_rules: Vec<IdPattern>) -> Result<Self> {
        Ok(Self {
            do_set: MatcherSet::new(do_rules)?,
            ignore_set: MatcherSet::new(ignore_rules)?,
        })
    }
}

#[derive(Debug, Clone, Default)]
struct ValueFilter {
    values: HashSet<String>,
}

impl ValueFilter {
    fn excludes(&self, value: &impl ToString) -> bool {
        !self.values.contains("*") && !self.values.contains(&value.to_string())
    }
}

#[derive(Debug)]
pub struct DtFilter {
    db_type: DbType,
    base_filters: Vec<BaseFilter>,
    events: ValueFilter,
    ddls: ValueFilter,
    dcls: ValueFilter,
    structures: ValueFilter,
    ignore_cmds: HashSet<String>,
    where_conditions: HashMap<RID, String>,
    cache: DashMap<RID, bool>,
}

impl DtFilter {
    /// Empty scope selects no objects; event/structure defaults match [filter].
    pub fn new(db_type: DbType) -> Self {
        Self {
            base_filters: vec![BaseFilter::default(); resource_levels(&db_type).len()],
            db_type,
            events: ValueFilter {
                values: HashSet::from(["*".into()]),
            },
            structures: ValueFilter {
                values: HashSet::from(["*".into()]),
            },
            ddls: ValueFilter::default(),
            dcls: ValueFilter::default(),
            ignore_cmds: HashSet::new(),
            where_conditions: HashMap::new(),
            cache: DashMap::new(),
        }
    }

    pub fn db_type(&self) -> &DbType {
        &self.db_type
    }

    pub fn set_base_filter(&mut self, kind: ResourceKind, filter: BaseFilter) -> Result<()> {
        let depth = resource_depth(&self.db_type, kind)
            .ok_or_else(|| anyhow::anyhow!("unsupported {kind:?} for {}", self.db_type))?;
        filter.do_set.validate_depth(depth)?;
        filter.ignore_set.validate_depth(depth)?;
        self.base_filters[depth - 1] = filter;
        self.cache.clear();
        Ok(())
    }

    pub fn add_do<I: ToRid + ?Sized>(&mut self, resource: &I) -> Result<()> {
        let id = resource.to_rid(&self.db_type)?;
        resource_kind(&self.db_type, &id)?;
        self.base_filters[id.len() - 1]
            .do_set
            .insert(IdPattern::literal(id))?;
        self.cache.clear();
        Ok(())
    }

    pub fn add_ignore<I: ToRid + ?Sized>(&mut self, resource: &I) -> Result<()> {
        let id = resource.to_rid(&self.db_type)?;
        resource_kind(&self.db_type, &id)?;
        self.base_filters[id.len() - 1]
            .ignore_set
            .insert(IdPattern::literal(id))?;
        self.cache.clear();
        Ok(())
    }

    pub fn filter<I: ToRid + ?Sized>(&self, resource: &I) -> Result<bool> {
        self.filter_rid(&resource.to_rid(&self.db_type)?)
    }

    /// Direct RID entry avoids a clone for callers already holding a validated identity.
    pub fn filter_rid(&self, id: &RID) -> Result<bool> {
        let kind = resource_kind(&self.db_type, id)?;
        if kind == ResourceKind::Column {
            let table = id.parent().expect("column resource has a parent");
            let columns = &self.base_filters[id.len() - 1];
            return Ok(self.filter_rid(&table)?
                || columns.ignore_set.matches(id)
                || (columns.do_set.matches_prefix(table.segments())
                    && !columns.do_set.matches(id)));
        }
        let cacheable = matches!(kind, ResourceKind::Table | ResourceKind::Collection);
        if cacheable {
            if let Some(value) = self.cache.get(id) {
                return Ok(*value);
            }
        }
        let scope_depth = resource_levels(&self.db_type)
            .iter()
            .take_while(|&&kind| kind != ResourceKind::Column)
            .count();
        let mut allowed = false;
        let mut ignored = false;
        for (index, base) in self.base_filters[..scope_depth].iter().enumerate() {
            if index < id.len() {
                let prefix = id.prefix(index + 1).expect("validated prefix depth");
                ignored |= base.ignore_set.matches(&prefix);
                allowed |= base.do_set.matches(&prefix);
            } else {
                // Descendant includes keep namespaces available for enumeration.
                allowed |= base.do_set.matches_prefix(id.segments());
                ignored |= base.ignore_set.covers_descendants(id.segments());
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
        Ok(self.events.excludes(event) || self.filter_rid(&id)?)
    }

    pub fn filter_ddl<I: ToRid + ?Sized>(&self, resource: &I, ddl: &DdlType) -> Result<bool> {
        let id = resource.to_rid(&self.db_type)?;
        resource_kind(&self.db_type, &id)?;
        Ok(self.ddls.excludes(ddl) || self.filter_rid(&id)?)
    }

    pub fn filter_dcl(&self, dcl: &DclType) -> bool {
        self.dcls.excludes(dcl)
    }
    pub fn filter_structure(&self, kind: &StructureType) -> bool {
        self.structures.excludes(kind)
    }
    pub fn filter_cmd(&self, command: &str) -> bool {
        self.ignore_cmds.contains(command)
    }

    pub fn where_condition<I: ToRid + ?Sized>(&self, table: &I) -> Result<Option<&str>> {
        let id = table.to_rid(&self.db_type)?;
        validate_rid(&self.db_type, ResourceKind::Table, &id)?;
        Ok(self.where_conditions.get(&id).map(String::as_str))
    }

    pub fn set_where_condition(&mut self, table: RID, condition: String) -> Result<()> {
        validate_rid(&self.db_type, ResourceKind::Table, &table)?;
        if let Some(previous) = self.where_conditions.get(&table) {
            ensure!(
                previous == &condition,
                "conflicting WHERE conditions for {table}"
            );
        }
        self.where_conditions.insert(table, condition);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use configparser::ini::Ini;

    use super::*;
    use crate::rid;

    fn ini(text: &str) -> Ini {
        let mut ini = Ini::new();
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(text.into()).unwrap();
        ini
    }

    fn filter(text: &str) -> DtFilter {
        DtFilter::from_ini(&ini(text), DbType::Mysql).unwrap()
    }

    #[test]
    fn namespace_discovery_and_projection_have_distinct_semantics() {
        let f = filter("[filter.table]\ndo=db1.t1,db1.t2\nignore=db1.t2\n[filter.column]\ndo=db1.t1.id,db2.t1.id\nignore=db1.t1.secret");
        assert!(!f.filter(&rid!("db1")).unwrap());
        assert!(f.filter(&rid!("db2")).unwrap());
        assert!(!f.filter(&rid!("db1", "t1", "id")).unwrap());
        assert!(f.filter(&rid!("db1", "t1", "other")).unwrap());
        assert!(f.filter(&rid!("db1", "t1", "secret")).unwrap());
        assert!(f.filter(&rid!("db1", "t2", "id")).unwrap());
        assert!(f.filter(&rid!("db2", "t1", "id")).unwrap());
        let f = filter("[filter.database]\ndo=db1\n[filter.column]\ndo=db1.t1.id");
        assert!(!f.filter(&rid!("db1", "t2", "other")).unwrap());
        let f = filter("[filter.database]\ndo=db1\n[filter.table]\nignore=db1.*");
        assert!(f.filter(&rid!("db1")).unwrap());
    }

    #[test]
    fn three_level_scope_and_cache_updates_keep_databases_isolated() {
        let mut f = DtFilter::from_ini(&ini("[filter.schema]\ndo=a.inventory,b.inventory\nignore=a.audit\n[filter.table]\nignore=b.inventory.order"), DbType::Mssql).unwrap();
        assert!(!f.filter(&rid!("a", "inventory", "order")).unwrap());
        assert!(f.filter(&rid!("b", "inventory", "order")).unwrap());
        f.add_ignore(&rid!("a")).unwrap();
        assert!(f.filter(&rid!("a", "inventory", "order")).unwrap());
        assert!(f.filter(&rid!("c")).unwrap());
        f.add_do(&rid!("c", "dbo", "new")).unwrap();
        assert!(!f.filter(&rid!("c")).unwrap());
        assert!(!f.filter(&rid!("c", "dbo", "new")).unwrap());
    }
}

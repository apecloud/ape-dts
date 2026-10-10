use std::collections::HashMap;

use anyhow::{ensure, Result};
use configparser::ini::Ini;

use super::config_enums::DbType;
use crate::resource::{resource_levels, ResourceKind};

#[derive(Clone, Default, Hash)]
pub struct FilterConfig {
    // User-facing do_dbs/ignore_dbs map to the database-specific top-level namespace.
    pub do_schemas: String,
    pub ignore_schemas: String,
    pub do_tbs: String,
    pub ignore_tbs: String,
    pub ignore_cols: String,
    pub do_events: String,
    pub do_structures: String,
    pub do_ddls: String,
    pub do_dcls: String,
    pub ignore_cmds: String,
    pub where_conditions: String,
}

/// Raw rules from one resource section. None means absent; Some("") means explicitly empty.
/// Patterns are compiled only when constructing a DtFilter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BaseFilterConfig {
    pub do_rules: Option<String>,
    pub ignore_rules: Option<String>,
}

impl BaseFilterConfig {
    pub fn from_ini(ini: &Ini, db_type: &DbType) -> Result<HashMap<ResourceKind, Self>> {
        Ok(resource_sections(ini, db_type)?
            .into_iter()
            .map(|(kind, section)| {
                (
                    kind,
                    Self {
                        do_rules: section.do_rules,
                        ignore_rules: section.ignore_rules,
                    },
                )
            })
            .collect())
    }
}

impl FilterConfig {
    /// Load [filter] as strings, including legacy resource rules and common options.
    /// Default event/structure selection and rule validation belong to DtFilter.
    pub fn from_ini(ini: &Ini) -> Self {
        Self {
            do_schemas: ini.get("filter", "do_dbs").unwrap_or_default(),
            ignore_schemas: ini.get("filter", "ignore_dbs").unwrap_or_default(),
            do_tbs: ini.get("filter", "do_tbs").unwrap_or_default(),
            ignore_tbs: ini.get("filter", "ignore_tbs").unwrap_or_default(),
            ignore_cols: ini.get("filter", "ignore_cols").unwrap_or_default(),
            do_events: ini.get("filter", "do_events").unwrap_or_default(),
            do_structures: ini.get("filter", "do_structures").unwrap_or_default(),
            do_ddls: ini.get("filter", "do_ddls").unwrap_or_default(),
            do_dcls: ini.get("filter", "do_dcls").unwrap_or_default(),
            ignore_cmds: ini.get("filter", "ignore_cmds").unwrap_or_default(),
            where_conditions: ini.get("filter", "where_conditions").unwrap_or_default(),
        }
    }
}

pub const BASE_FILTER_SECTIONS: &[(ResourceKind, &str)] = &[
    (ResourceKind::Database, "filter.database"),
    (ResourceKind::Schema, "filter.schema"),
    (ResourceKind::Table, "filter.table"),
    (ResourceKind::Column, "filter.column"),
    (ResourceKind::Collection, "filter.collection"),
    (ResourceKind::Topic, "filter.topic"),
];

#[derive(Debug, Clone, Default)]
pub(super) struct ResourceSectionConfig {
    pub do_rules: Option<String>,
    pub ignore_rules: Option<String>,
    pub router: Option<String>,
}

pub(super) fn resource_sections(
    ini: &Ini,
    db_type: &DbType,
) -> Result<HashMap<ResourceKind, ResourceSectionConfig>> {
    let levels = resource_levels(db_type);
    let mut result = HashMap::<ResourceKind, ResourceSectionConfig>::new();
    for (section, fields) in ini.get_map_ref() {
        if !section.starts_with("filter.") {
            continue;
        }
        let kind = match section.as_str() {
            "filter.db" if !matches!(db_type, DbType::Kafka) => levels[0],
            "filter.document" => ResourceKind::Collection,
            _ => BASE_FILTER_SECTIONS.iter().find(|(_, name)| name == section).map(|(kind, _)| *kind)
                .ok_or_else(|| anyhow::anyhow!("unknown resource section [{section}]; WHERE and category options belong in [filter]"))?,
        };
        for key in fields.keys() {
            ensure!(["do", "ignore", "router"].contains(&key.as_str()), "unsupported [{section}].{key}; resource sections only accept do/ignore/router; WHERE and categories belong in [filter]");
        }
        if !levels.contains(&kind) {
            ensure!(
                fields
                    .values()
                    .all(|v| v.as_deref().unwrap_or("").trim().is_empty()),
                "[{section}] is not supported by {db_type}"
            );
            continue;
        };
        let base = result.entry(kind).or_default();
        for (key, slot) in [
            ("do", &mut base.do_rules),
            ("ignore", &mut base.ignore_rules),
            ("router", &mut base.router),
        ] {
            if let Some(value) = fields.get(key) {
                let value = value.as_deref().unwrap_or("").trim().to_string();
                if let Some(previous) = slot.as_ref() {
                    ensure!(previous == &value, "conflicting aliases for {kind:?}.{key}");
                }
                *slot = Some(value);
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ini(text: &str) -> Ini {
        let mut ini = Ini::new();
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(text.into()).unwrap();
        ini
    }

    #[test]
    fn resource_loading_preserves_raw_rules_and_field_presence() {
        let ini = ini("[filter]\ndo_events=unknown\nwhere_conditions=json:invalid\n[filter.db]\ndo=db.r#[#\n[filter.database]\ndo=db.r#[#\n[filter.table]\nignore=\nrouter=invalid:route:rule");
        let bases = BaseFilterConfig::from_ini(&ini, &DbType::Mysql).unwrap();
        assert_eq!(bases.len(), 2);
        assert_eq!(
            bases[&ResourceKind::Database].do_rules.as_deref(),
            Some("db.r#[#")
        );
        assert_eq!(
            bases[&ResourceKind::Table].ignore_rules.as_deref(),
            Some("")
        );
        assert_eq!(bases[&ResourceKind::Table].do_rules, None);
        let options = FilterConfig::from_ini(&ini);
        assert_eq!(options.do_events, "unknown");
        assert_eq!(options.where_conditions, "json:invalid");
        assert!(options.do_ddls.is_empty());
    }

    #[test]
    fn resource_loading_validates_sections_and_aliases_without_compiling_rules() {
        for (db_type, text) in [
            (DbType::Mysql, "[filter.table]\nwhere_conditions="),
            (DbType::Mysql, "[filter.dcl]\ndo=grant"),
            (DbType::Mysql, "[filter.db]\ndo=a\n[filter.database]\ndo=b"),
            (DbType::Pg, "[filter.database]\ndo=db"),
            (DbType::Mongo, "[filter.column]\nignore=db.c.field"),
            (DbType::Kafka, "[filter.db]\ndo=topic"),
        ] {
            assert!(
                BaseFilterConfig::from_ini(&ini(text), &db_type).is_err(),
                "{text}"
            );
        }
        let bases = BaseFilterConfig::from_ini(
            &ini("[filter.db]\ndo=schema\n[filter.database]\ndo="),
            &DbType::Pg,
        )
        .unwrap();
        assert_eq!(bases.len(), 1);
        assert_eq!(
            bases[&ResourceKind::Schema].do_rules.as_deref(),
            Some("schema")
        );
        let bases =
            BaseFilterConfig::from_ini(&ini("[filter.document]\ndo=db.c"), &DbType::Mongo).unwrap();
        assert!(bases.contains_key(&ResourceKind::Collection));
    }
}

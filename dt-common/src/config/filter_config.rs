use std::collections::BTreeMap;

use anyhow::{ensure, Result};
use configparser::ini::Ini;

use super::router_config::{ROUTE, TO_PREFIX};
use crate::resource::{RESOURCE_SEPARATOR, WILDCARD};

pub const FILTER: &str = "filter";
pub const RULES: &str = "rules";
pub const DO: &str = "do";
pub const IGNORE: &str = "ignore";
pub const WHERE_CONDITIONS: &str = "where_conditions";

/// Any `[rules.*]` section, even empty, selects Basic and disables legacy `[filter]`.
#[derive(Debug, Clone, Hash)]
pub enum FilterConfig {
    Legacy(LegacyFilterConfig),
    Basic(BTreeMap<String, FilterSectionConfig>),
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self::Legacy(LegacyFilterConfig::default())
    }
}

#[derive(Debug, Clone, Default, Hash)]
pub struct LegacyFilterConfig {
    /// `do_dbs=inventory*` selects the engine's top-level namespace.
    pub do_schemas: String,
    pub ignore_schemas: String,
    pub do_tbs: String,
    pub ignore_tbs: String,
    /// `ignore_cols=json:[{"db":"inventory","tb":"orders","ignore_cols":["secret"]}]`
    pub ignore_cols: String,
    pub do_events: String,
    pub do_structures: String,
    pub do_ddls: String,
    pub do_dcls: String,
    pub ignore_cmds: String,
    /// `where_conditions=json:[{"db":"inventory","tb":"orders","condition":"id > 100"}]`
    pub where_conditions: String,
}

/// Raw `[rules.*]` values: `None` is absent; `Some("")` is explicitly empty.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct FilterSectionConfig {
    /// `do=inventory.*,archive.orders` in `[rules.table]`; `do=insert,update` in `[rules.event]`.
    pub do_rules: Option<String>,
    /// `ignore=inventory.secret` in `[rules.table]`; `ignore=SET,DEL` in `[rules.cmd]`.
    pub ignore_rules: Option<String>,
    pub expression_filters: Vec<ExpressionFilterConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExpressionFilterConfig {
    /// In `[rules.table]`: `where_conditions=inventory.orders:"id > 100",inventory.events:"kind = \"sale\""`.
    /// RID keys use SQL quoting, never patterns; duplicates keep the first value.
    /// Expression quotes are our engine-independent escape, using JSON rules: `\"` for `"`, `\\` for `\`.
    WhereConditions(String),
}

impl FilterSectionConfig {
    pub fn from_ini(ini: &Ini) -> Result<BTreeMap<String, Self>> {
        let prefix = format!("{RULES}{RESOURCE_SEPARATOR}");
        let mut result = BTreeMap::new();
        for (section, fields) in ini.get_map_ref() {
            let Some(name) = section.strip_prefix(prefix.as_str()) else {
                continue;
            };
            for key in fields.keys() {
                ensure!([DO, IGNORE, ROUTE, WHERE_CONDITIONS].contains(&key.as_str()) || key.starts_with(TO_PREFIX), "unsupported [{section}].{key}; rules sections only accept do/ignore/route/where_conditions/to_<kind>");
            }
            let value = |key: &str| {
                fields
                    .get(key)
                    .map(|value| value.as_deref().unwrap_or_default().trim().to_owned())
            };
            result.insert(
                name.to_owned(),
                Self {
                    do_rules: value(DO),
                    ignore_rules: value(IGNORE),
                    expression_filters: value(WHERE_CONDITIONS)
                        .map(ExpressionFilterConfig::WhereConditions)
                        .into_iter()
                        .collect(),
                },
            );
        }
        Ok(result)
    }
}

impl LegacyFilterConfig {
    /// Missing or empty event/structure options select all.
    pub fn from_ini(ini: &Ini) -> Self {
        Self {
            do_schemas: ini.get(FILTER, "do_dbs").unwrap_or_default(),
            ignore_schemas: ini.get(FILTER, "ignore_dbs").unwrap_or_default(),
            do_tbs: ini.get(FILTER, "do_tbs").unwrap_or_default(),
            ignore_tbs: ini.get(FILTER, "ignore_tbs").unwrap_or_default(),
            ignore_cols: ini.get(FILTER, "ignore_cols").unwrap_or_default(),
            do_events: ini
                .get(FILTER, "do_events")
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| WILDCARD.into()),
            do_structures: ini
                .get(FILTER, "do_structures")
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| WILDCARD.into()),
            do_ddls: ini.get(FILTER, "do_ddls").unwrap_or_default(),
            do_dcls: ini.get(FILTER, "do_dcls").unwrap_or_default(),
            ignore_cmds: ini.get(FILTER, "ignore_cmds").unwrap_or_default(),
            where_conditions: ini.get(FILTER, WHERE_CONDITIONS).unwrap_or_default(),
        }
    }
}

impl FilterConfig {
    pub fn from_ini(ini: &Ini) -> Result<Self> {
        let sections = FilterSectionConfig::from_ini(ini)?;
        if sections.is_empty() {
            Ok(Self::Legacy(LegacyFilterConfig::from_ini(ini)))
        } else {
            Ok(Self::Basic(sections))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::router_config::RouterConfig;

    fn ini(text: &str) -> Ini {
        let mut ini = Ini::new();
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(text.into()).unwrap();
        ini
    }

    #[test]
    fn section_presence_selects_the_same_format_for_filter_and_router() {
        for (text, basic) in [
            ("", false),
            ("[filter]\ndo_tbs=db.t\n[router]\ntb_map=db.t:archive.t", false),
            ("[rules_extra.table]\ndo=db.t", false),
            ("[rules]\ndo=db.t", false),
            ("[filter.table]\ndo=db.t", false),
            ("[rules.table]", true),
            ("[rules.event]\ndo=insert", true),
            ("[rules.table]\nroute=db.t:archive.t", true),
            ("[rules.table]\nto_topic=db.t:events", true),
            ("[rules.future]", true),
            ("[filter]\nwhere_conditions=json:invalid\n[router]\ncol_map=json:invalid\n[rules.table]\ndo=", true),
        ] {
            let input = ini(text);
            assert_eq!(matches!(FilterConfig::from_ini(&input).unwrap(), FilterConfig::Basic(_)), basic, "{text}");
            assert_eq!(matches!(RouterConfig::from_ini(&input).unwrap(), RouterConfig::Basic(_)), basic, "{text}");
        }
    }

    #[test]
    fn legacy_keys_and_selection_defaults_survive_loading() {
        for (selection, events, structures) in [
            ("", "*", "*"),
            ("do_events=\ndo_structures=", "*", "*"),
            ("do_events\ndo_structures", "*", "*"),
            ("do_events=insert\ndo_structures=table", "insert", "table"),
        ] {
            let config = FilterConfig::from_ini(&ini(&format!(
                "[filter]\ndo_dbs=source\nignore_dbs=ignored\ndo_tbs=source.*\nwhere_conditions=json:invalid\n{selection}"
            ))).unwrap();
            let FilterConfig::Legacy(options) = config else {
                panic!("{selection}")
            };
            assert_eq!(options.do_schemas, "source");
            assert_eq!(options.ignore_schemas, "ignored");
            assert_eq!(options.do_tbs, "source.*");
            assert_eq!(options.where_conditions, "json:invalid");
            assert_eq!(options.do_events, events, "{selection}");
            assert_eq!(options.do_structures, structures, "{selection}");
            assert!(options.do_ddls.is_empty());
            assert!(options.do_dcls.is_empty());
        }
    }

    #[test]
    fn resource_loading_preserves_raw_values_and_field_presence() {
        let config = FilterConfig::from_ini(&ini(r#"[filter]
where_conditions=json:invalid
[rules.db]
do=db.r#[#
where_conditions=inventory:"name = \"orders\""
[rules.schema]
do=db.r#[#
[rules.table]
ignore=
route=invalid:route:rule
where_conditions=
[rules.column]
[rules.future]
do=new
[rules.event]
do=invalid_event
"#))
        .unwrap();
        let FilterConfig::Basic(sections) = config else {
            panic!("expected Basic")
        };
        for (kind, do_rules, ignore, condition) in [
            (
                "db",
                Some("db.r#[#"),
                None,
                Some(r#"inventory:"name = \"orders\"""#),
            ),
            ("schema", Some("db.r#[#"), None, None),
            ("table", None, Some(""), Some("")),
            ("column", None, None, None),
            ("future", Some("new"), None, None),
            ("event", Some("invalid_event"), None, None),
        ] {
            let section = &sections[kind];
            assert_eq!(section.do_rules.as_deref(), do_rules, "{kind}");
            assert_eq!(section.ignore_rules.as_deref(), ignore, "{kind}");
            assert_eq!(
                section.expression_filters,
                condition
                    .map(|s| ExpressionFilterConfig::WhereConditions(s.into()))
                    .into_iter()
                    .collect::<Vec<_>>(),
                "{kind}"
            );
        }
    }

    #[test]
    fn both_readers_reject_unknown_fields_and_the_old_router_key() {
        for key in ["unknown", "router", "topic_map"] {
            let input = ini(&format!("[rules.table]\n{key}="));
            assert!(FilterConfig::from_ini(&input).is_err(), "{key}");
            assert!(RouterConfig::from_ini(&input).is_err(), "{key}");
        }
    }
}

use std::collections::BTreeMap;

use anyhow::{ensure, Result};
use configparser::ini::Ini;

use super::filter_config::{EXCLUDE, INCLUDE, SOURCE, WHERE_CONDITIONS};
use crate::resource::RESOURCE_SEPARATOR;

pub const ROUTER: &str = "router";
pub const ROUTE: &str = "route";
pub const TO_PREFIX: &str = "to_";

/// Any `[source.*]` section disables legacy `[router]`, even when empty.
#[derive(Debug, Clone, Hash)]
pub enum RouterConfig {
    Legacy(LegacyRouterConfig),
    Basic(BTreeMap<String, RouterSectionConfig>),
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self::Legacy(LegacyRouterConfig::default())
    }
}

#[derive(Debug, Clone, Default, Hash)]
pub struct LegacyRouterConfig {
    /// `db_map=inventory:archive` maps the engine's top-level namespace.
    pub schema_map: String,
    /// `tb_map=inventory.orders:archive.orders`
    pub tb_map: String,
    /// `col_map=json:[{"db":"inventory","tb":"orders","col_map":{"id":"order_id"}}]`
    pub col_map: String,
    /// `topic_map=inventory.orders:orders,inventory.*:shared,*.*:fallback`
    pub topic_map: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct RouterSectionConfig {
    /// `route=inventory.orders:archive.orders` in `[source.table]`.
    pub route: Option<String>,
    /// `[source.table]`: `to_topic=inventory.orders:orders,inventory.*:shared`.
    pub to: BTreeMap<String, String>,
}

impl RouterSectionConfig {
    pub fn from_ini(ini: &Ini) -> Result<BTreeMap<String, Self>> {
        let prefix = format!("{SOURCE}{RESOURCE_SEPARATOR}");
        let mut result = BTreeMap::new();
        for (section, fields) in ini.get_map_ref() {
            let Some(name) = section.strip_prefix(prefix.as_str()) else {
                continue;
            };
            for key in fields.keys() {
                ensure!([INCLUDE, EXCLUDE, ROUTE, WHERE_CONDITIONS].contains(&key.as_str()) || key.starts_with(TO_PREFIX), "unsupported [{section}].{key}; source sections only accept include/exclude/route/where_conditions/to_<kind>");
            }
            result.insert(
                name.to_owned(),
                Self {
                    route: fields
                        .get(ROUTE)
                        .map(|value| value.as_deref().unwrap_or_default().trim().to_owned()),
                    to: fields
                        .iter()
                        .filter(|(key, _)| key.starts_with(TO_PREFIX))
                        .map(|(key, value)| {
                            (
                                key.clone(),
                                value.as_deref().unwrap_or_default().trim().to_owned(),
                            )
                        })
                        .collect(),
                },
            );
        }
        Ok(result)
    }
}

impl LegacyRouterConfig {
    pub fn from_ini(ini: &Ini) -> Self {
        Self {
            schema_map: ini.get(ROUTER, "db_map").unwrap_or_default(),
            tb_map: ini.get(ROUTER, "tb_map").unwrap_or_default(),
            col_map: ini.get(ROUTER, "col_map").unwrap_or_default(),
            topic_map: ini.get(ROUTER, "topic_map").unwrap_or_default(),
        }
    }
}

impl RouterConfig {
    pub fn from_ini(ini: &Ini) -> Result<Self> {
        let sections = RouterSectionConfig::from_ini(ini)?;
        if sections.is_empty() {
            Ok(Self::Legacy(LegacyRouterConfig::from_ini(ini)))
        } else {
            Ok(Self::Basic(sections))
        }
    }
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
    fn routing_fields_remain_raw_and_preserve_absent_or_empty_values() {
        let input = ini("[router]\ncol_map=json:invalid\n[source.db]\nroute=a:b:c\n[source.schema]\nroute=a:c\n[source.table]\nroute=\nwhere_conditions=json:invalid\nto_topic=db.orders:test1\nto_future=not:a:map\nto_db=\n[source.column]\ninclude=not.a.valid.column\n[source.future]\nroute=new:old");
        let config = RouterConfig::from_ini(&input).unwrap();
        let RouterConfig::Basic(sections) = config else {
            panic!("expected Basic")
        };
        for (name, value) in [
            ("db", Some("a:b:c")),
            ("schema", Some("a:c")),
            ("table", Some("")),
            ("column", None),
            ("future", Some("new:old")),
        ] {
            assert_eq!(sections[name].route.as_deref(), value, "{name}");
        }
        for (key, value) in [
            ("to_topic", "db.orders:test1"),
            ("to_future", "not:a:map"),
            ("to_db", ""),
        ] {
            assert_eq!(sections["table"].to[key], value, "{key}");
        }
        assert!(crate::config::filter_config::FilterConfig::from_ini(&input).is_ok());
    }

    #[test]
    fn legacy_keys_defaults_and_invalid_json_remain_raw() {
        for (field, expected) in [
            ("", ""),
            ("topic_map", ""),
            ("topic_map=", ""),
            ("topic_map=*.*:events", "*.*:events"),
            ("topic_map=json:invalid", "json:invalid"),
        ] {
            let config = RouterConfig::from_ini(&ini(&format!(
                "[router]\ndb_map=source:target\ntb_map=not-a-map\ncol_map=json:invalid\n{field}"
            )))
            .unwrap();
            let RouterConfig::Legacy(options) = config else {
                panic!("{field}")
            };
            assert_eq!(options.schema_map, "source:target");
            assert_eq!(options.tb_map, "not-a-map");
            assert_eq!(options.col_map, "json:invalid");
            assert_eq!(options.topic_map, expected, "{field}");
        }
    }
}

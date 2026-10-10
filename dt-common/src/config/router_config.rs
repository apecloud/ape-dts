use std::collections::HashMap;

use anyhow::Result;
use configparser::ini::Ini;

use super::{config_enums::DbType, filter_config::resource_sections};
use crate::resource::ResourceKind;

#[derive(Clone, Hash)]
pub enum RouterConfig {
    Rdb {
        // User-facing db_map maps the database-specific top-level namespace.
        schema_map: String,
        tb_map: String,
        col_map: String,
        topic_map: String,
    },
}

/// Raw object-routing rules from one resource section.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BaseRouterConfig {
    pub router: Option<String>,
}

impl BaseRouterConfig {
    pub fn from_ini(ini: &Ini, src_db_type: &DbType) -> Result<HashMap<ResourceKind, Self>> {
        Ok(resource_sections(ini, src_db_type)?
            .into_iter()
            .map(|(kind, section)| {
                (
                    kind,
                    Self {
                        router: section.router,
                    },
                )
            })
            .collect())
    }
}

impl RouterConfig {
    /// Load legacy [router] rules without decoding identifiers or mappings.
    pub fn from_ini(ini: &Ini) -> Self {
        Self::Rdb {
            schema_map: ini.get("router", "db_map").unwrap_or_default(),
            tb_map: ini.get("router", "tb_map").unwrap_or_default(),
            col_map: ini.get("router", "col_map").unwrap_or_default(),
            topic_map: ini.get("router", "topic_map").unwrap_or_default(),
        }
    }
}

/// Topic dispatch has a default rule as well as resource selectors, so it stays
/// in [router] rather than an object resource section.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicRouterConfig {
    pub topic_map: String,
    pub dispatch: Option<String>,
}

impl TopicRouterConfig {
    pub fn from_ini(ini: &Ini, src_db_type: &DbType) -> Result<Self> {
        resource_sections(ini, src_db_type)?;
        Ok(Self {
            topic_map: ini.get("router", "topic_map").unwrap_or_default(),
            dispatch: ini.get("router", "dispatch"),
        })
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
    fn routing_config_preserves_raw_values_and_absent_or_empty_routes() {
        let ini = ini("[filter.db]\nrouter=a:b:c\n[filter.database]\nrouter=a:b:c\n[filter.table]\nrouter=\n[filter.column]\ndo=not.a.valid.column\n[router]\ndb_map=a:b\ntb_map=not-a-map\ncol_map=json:invalid\ntopic_map=*.*:topic");
        let bases = BaseRouterConfig::from_ini(&ini, &DbType::Mysql).unwrap();
        assert_eq!(
            bases[&ResourceKind::Database].router.as_deref(),
            Some("a:b:c")
        );
        assert_eq!(bases[&ResourceKind::Table].router.as_deref(), Some(""));
        assert_eq!(bases[&ResourceKind::Column].router, None);
        let RouterConfig::Rdb {
            schema_map,
            tb_map,
            col_map,
            topic_map,
        } = RouterConfig::from_ini(&ini);
        assert_eq!(schema_map, "a:b");
        assert_eq!(tb_map, "not-a-map");
        assert_eq!(col_map, "json:invalid");
        assert_eq!(topic_map, "*.*:topic");
        assert!(BaseRouterConfig::from_ini(
            &self::ini("[filter.db]\nrouter=a:b\n[filter.database]\nrouter=a:c"),
            &DbType::Mysql
        )
        .is_err());
    }

    #[test]
    fn dispatch_loading_preserves_presence_and_defers_json_validation() {
        for (suffix, expected) in [
            ("", None),
            ("\ndispatch", None),
            ("\ndispatch=", Some("")),
            ("\ndispatch=json:invalid", Some("json:invalid")),
        ] {
            let config = TopicRouterConfig::from_ini(
                &ini(&format!("[router]\ntopic_map=*.*:topic{suffix}")),
                &DbType::Mysql,
            )
            .unwrap();
            assert_eq!(config.topic_map, "*.*:topic");
            assert_eq!(config.dispatch.as_deref(), expected);
        }
        assert!(TopicRouterConfig::from_ini(
            &ini("[filter.topic]\ndispatch=*.*:topic"),
            &DbType::Mysql
        )
        .is_err());
    }
}

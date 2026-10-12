use std::collections::HashMap;

use anyhow::{ensure, Result};

use super::DtFilter;
use crate::resource::{resource_depth, resource_kind, validate_rid, ResourceKind, ToRid, RID};

#[derive(Debug, Clone)]
pub enum ExpressionFilter {
    WhereConditions {
        /// Exact RIDs; duplicate keys keep the first condition.
        conditions: HashMap<RID, String>,
    },
}

impl DtFilter {
    /// Exact lookup; parent conditions are not inherited.
    pub fn where_condition<I: ToRid + ?Sized>(&self, resource: &I) -> Result<Option<&str>> {
        let id = resource.to_rid(&self.db_type)?;
        let kind = resource_kind(&self.db_type, &id)?;
        Ok(self
            .expression_filters
            .get(&kind)
            .and_then(|filter| match filter {
                ExpressionFilter::WhereConditions { conditions } => {
                    conditions.get(&id).map(String::as_str)
                }
            }))
    }

    /// Validate all conditions before merging.
    pub fn set_expression_filter(
        &mut self,
        kind: ResourceKind,
        filter: ExpressionFilter,
    ) -> Result<()> {
        ensure!(
            resource_depth(&self.db_type, kind).is_some(),
            "unsupported {kind:?} for {}",
            self.db_type
        );
        match filter {
            ExpressionFilter::WhereConditions { conditions } => {
                for id in conditions.keys() {
                    validate_rid(&self.db_type, kind, id)?;
                }
                let ExpressionFilter::WhereConditions {
                    conditions: previous,
                } = self.expression_filters.entry(kind).or_insert_with(|| {
                    ExpressionFilter::WhereConditions {
                        conditions: HashMap::new(),
                    }
                });
                for (id, condition) in conditions {
                    previous.entry(id).or_insert(condition);
                }
            }
        }
        Ok(())
    }

    pub fn set_where_condition(&mut self, id: RID, condition: String) -> Result<()> {
        let kind = resource_kind(&self.db_type, &id)?;
        let ExpressionFilter::WhereConditions { conditions } = self
            .expression_filters
            .entry(kind)
            .or_insert_with(|| ExpressionFilter::WhereConditions {
                conditions: HashMap::new(),
            });
        conditions.entry(id).or_insert(condition);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::config_enums::DbType, rid};

    #[test]
    fn lookup_uses_only_the_exact_resource_level_and_id() {
        let mut filter = DtFilter::new(DbType::Mssql);
        filter
            .set_where_condition(rid!("db"), "db condition".into())
            .unwrap();
        filter
            .set_where_condition(rid!("db", "s", "t"), "table condition".into())
            .unwrap();
        for (id, expected) in [
            (rid!("db"), Some("db condition")),
            (rid!("other"), None),
            (rid!("db", "s"), None),
            (rid!("db", "s", "t"), Some("table condition")),
            (rid!("db", "s", "other"), None),
            (rid!("db", "s", "t", "id"), None),
        ] {
            assert_eq!(filter.where_condition(&id).unwrap(), expected, "{id}");
        }
        assert!(filter
            .where_condition(&rid!("db", "s", "t", "id", "extra"))
            .is_err());
    }

    #[test]
    fn added_where_ids_and_quoted_config_names_are_exact() {
        use configparser::ini::Ini;

        use crate::{config::filter_config::FilterConfig, utils::sql_util::SqlUtil};
        for engine in [DbType::Mysql, DbType::Pg, DbType::Mssql] {
            let parent = if engine == DbType::Mssql {
                rid!("db", "sales")
            } else {
                rid!("db")
            };
            let names = [
                "order*",
                "event?",
                "r#order[0-9]+#",
                "r#[#",
                "a.b,c:d",
                "quote`\"[]",
            ];
            let mut filter = DtFilter::new(engine.clone());
            let mut rules = Vec::new();
            for name in names {
                let id = parent.child(name).unwrap();
                filter.set_where_condition(id.clone(), name.into()).unwrap();
                rules.push(format!(
                    "{}:{}",
                    id.segments()
                        .iter()
                        .map(|segment| SqlUtil::escape_by_db_type(segment, &engine))
                        .collect::<Vec<_>>()
                        .join("."),
                    serde_json::to_string(name).unwrap()
                ));
            }
            let mut ini = Ini::new();
            ini.set_inline_comment_symbols(Some(&Vec::new()));
            ini.read(format!(
                "[rules.table]\nwhere_conditions={}",
                rules.join(",")
            ))
            .unwrap();
            let configured =
                DtFilter::from_config(&FilterConfig::from_ini(&ini).unwrap(), engine).unwrap();
            for f in [&filter, &configured] {
                for name in names {
                    assert_eq!(
                        f.where_condition(&parent.child(name).unwrap()).unwrap(),
                        Some(name)
                    );
                }
                for name in ["orders", "order12", "event", "event1", "unselected"] {
                    assert_eq!(
                        f.where_condition(&parent.child(name).unwrap()).unwrap(),
                        None
                    );
                }
                assert_eq!(f.where_condition(&parent).unwrap(), None);
            }
        }
    }

    #[test]
    fn expression_updates_keep_first_conditions_and_validate_before_merging() {
        let mut filter = DtFilter::new(DbType::Mysql);
        let table = rid!("db", "t");
        for condition in ["id > 1", "id > 1", "id > 2"] {
            filter
                .set_where_condition(table.clone(), condition.into())
                .unwrap();
        }
        assert_eq!(filter.where_condition(&table).unwrap(), Some("id > 1"));
        assert!(filter
            .set_where_condition(rid!("db", "t", "id", "extra"), "invalid".into())
            .is_err());
        for (kind, conditions) in [
            (ResourceKind::Schema, HashMap::new()),
            (
                ResourceKind::Table,
                HashMap::from([
                    (rid!("db", "other"), "must not be added".into()),
                    (rid!("db"), "invalid depth".into()),
                ]),
            ),
        ] {
            assert!(filter
                .set_expression_filter(kind, ExpressionFilter::WhereConditions { conditions })
                .is_err());
            assert_eq!(filter.where_condition(&table).unwrap(), Some("id > 1"));
            assert_eq!(filter.where_condition(&rid!("db", "other")).unwrap(), None);
            assert_eq!(filter.expression_filters.len(), 1);
        }
        filter
            .set_where_condition(rid!("db"), "db condition".into())
            .unwrap();
        filter
            .set_expression_filter(
                ResourceKind::Table,
                ExpressionFilter::WhereConditions {
                    conditions: HashMap::from([
                        (table.clone(), "later".into()),
                        (rid!("db", "other"), "other".into()),
                    ]),
                },
            )
            .unwrap();
        assert_eq!(filter.where_condition(&table).unwrap(), Some("id > 1"));
        assert_eq!(
            filter.where_condition(&rid!("db", "other")).unwrap(),
            Some("other")
        );
        assert_eq!(
            filter.where_condition(&rid!("db")).unwrap(),
            Some("db condition")
        );
        assert_eq!(filter.expression_filters.len(), 2);
    }
}

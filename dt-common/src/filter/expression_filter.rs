use std::collections::HashMap;

use anyhow::Result;

use super::DtFilter;
use crate::resource::{ResourceKind, RID};

#[derive(Debug, Clone)]
pub enum ExpressionFilter {
    WhereConditions { conditions: HashMap<RID, String> },
}

impl DtFilter {
    /// Exact, level-local lookup; duplicate RIDs keep the first condition.
    pub fn where_condition(&self, id: impl Into<RID>) -> Result<Option<&str>> {
        let id = id.into();
        let Some(kind) = ResourceKind::from_rid(&self.db_type, &id) else {
            return Ok(None);
        };
        Ok(self
            .expression_filters
            .get(&kind)
            .and_then(|filter| match filter {
                ExpressionFilter::WhereConditions { conditions } => {
                    conditions.get(&id).map(String::as_str)
                }
            }))
    }

    pub fn set_expression_filter(
        &mut self,
        kind: ResourceKind,
        filter: ExpressionFilter,
    ) -> Result<()> {
        match filter {
            ExpressionFilter::WhereConditions { conditions } => {
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
        let Some(kind) = ResourceKind::from_rid(&self.db_type, &id) else {
            return Ok(());
        };
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
    fn expression_updates_keep_first_conditions_without_depth_checks() {
        let mut filter = DtFilter::new(DbType::Mysql);
        let table = rid!("db", "t");
        for condition in ["id > 1", "id > 1", "id > 2"] {
            filter
                .set_where_condition(table.clone(), condition.into())
                .unwrap();
        }
        assert_eq!(filter.where_condition(&table).unwrap(), Some("id > 1"));
        filter
            .set_where_condition(rid!("db", "t", "id", "extra"), "unused".into())
            .unwrap();
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
                        (rid!("db"), "different level".into()),
                    ]),
                },
            )
            .unwrap();
        assert_eq!(filter.where_condition(&table).unwrap(), Some("id > 1"));
        assert_eq!(
            filter.where_condition(rid!("db", "other")).unwrap(),
            Some("other")
        );
        assert_eq!(
            filter.where_condition(rid!("db")).unwrap(),
            Some("db condition")
        );
        assert_eq!(filter.expression_filters.len(), 2);
    }
}

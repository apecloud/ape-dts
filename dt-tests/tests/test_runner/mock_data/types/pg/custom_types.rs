use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::test_runner::mock_data::{
    context::MockDbContext, mock_stmt::MockColType, pg_type::PgType, random::Random,
};

#[derive(Hash, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PgCustomType {
    Enum {
        name: String,
        labels: Vec<String>,
    },
    Domain {
        name: String,
        base: std::boxed::Box<PgType>,
        check: Option<String>,
        #[serde(default)]
        values: Vec<String>,
    },
    Composite {
        name: String,
        fields: Vec<PgCustomField>,
        #[serde(default)]
        values: Vec<Vec<String>>,
    },
    Range {
        name: String,
        subtype: std::boxed::Box<PgType>,
        subtype_opclass: Option<String>,
        collation: Option<String>,
        canonical: Option<String>,
        subtype_diff: Option<String>,
        multirange_type_name: Option<String>,
        #[serde(default)]
        values: Vec<String>,
    },
}

#[derive(Hash, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgCustomField {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: PgType,
}

impl PgCustomType {
    pub(crate) fn name(&self) -> &str {
        match self {
            PgCustomType::Enum { name, .. }
            | PgCustomType::Domain { name, .. }
            | PgCustomType::Composite { name, .. }
            | PgCustomType::Range { name, .. } => name,
        }
    }

    pub(crate) fn type_name(&self, db: &str, ctx: &MockDbContext) -> String {
        format!(
            "{}.{}",
            PgType::quote_identifier(db, ctx),
            PgType::quote_identifier(self.name(), ctx)
        )
    }

    fn ddl_stmt(&self, db: &str, ctx: &MockDbContext) -> String {
        let type_name = self.type_name(db, ctx);
        match self {
            PgCustomType::Enum { labels, .. } => {
                let labels = labels
                    .iter()
                    .map(|label| single_quote(&Self::escape_sql_string(label)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("CREATE TYPE {} AS ENUM ({});", type_name, labels)
            }
            PgCustomType::Domain {
                base,
                check,
                values: _,
                ..
            } => {
                let check = check
                    .as_ref()
                    .map(|expr| format!(" CHECK ({})", expr))
                    .unwrap_or_default();
                format!(
                    "CREATE DOMAIN {} AS {}{};",
                    type_name,
                    base.type_name(db, ctx),
                    check
                )
            }
            PgCustomType::Composite { fields, .. } => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        format!(
                            "{} {}",
                            PgType::quote_identifier(&field.name, ctx),
                            field.col_type.type_name(db, ctx)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("CREATE TYPE {} AS ({});", type_name, fields)
            }
            PgCustomType::Range {
                subtype,
                subtype_opclass,
                collation,
                canonical,
                subtype_diff,
                multirange_type_name,
                ..
            } => {
                let mut attrs = vec![format!("SUBTYPE = {}", subtype.type_name(db, ctx))];
                if let Some(subtype_opclass) = subtype_opclass {
                    attrs.push(format!("SUBTYPE_OPCLASS = {}", subtype_opclass));
                }
                if let Some(collation) = collation {
                    attrs.push(format!("COLLATION = {}", collation));
                }
                if let Some(canonical) = canonical {
                    attrs.push(format!("CANONICAL = {}", canonical));
                }
                if let Some(subtype_diff) = subtype_diff {
                    attrs.push(format!("SUBTYPE_DIFF = {}", subtype_diff));
                }
                if let Some(multirange_type_name) = multirange_type_name {
                    attrs.push(format!("MULTIRANGE_TYPE_NAME = {}", multirange_type_name));
                }
                format!("CREATE TYPE {} AS RANGE ({});", type_name, attrs.join(", "))
            }
        }
    }

    pub(crate) fn next_value_str(
        &self,
        db: &str,
        ctx: &MockDbContext,
        random: &mut Random,
    ) -> String {
        let type_name = self.type_name(db, ctx);
        match self {
            PgCustomType::Enum { labels, .. } => {
                let label = labels[random.random_range(0..labels.len() as i32) as usize].as_str();
                format!(
                    "{}::{}",
                    single_quote(&Self::escape_sql_string(label)),
                    type_name
                )
            }
            PgCustomType::Domain { base, values, .. } => {
                let value = if values.is_empty() {
                    base.next_value_str(db, ctx, random)
                } else {
                    values[random.random_range(0..values.len() as i32) as usize].clone()
                };
                format!("{}::{}", value, type_name)
            }
            PgCustomType::Composite { fields, values, .. } => {
                let values = if values.is_empty() {
                    fields
                        .iter()
                        .map(|field| field.col_type.next_value_str(db, ctx, random))
                        .collect::<Vec<_>>()
                } else {
                    values[random.random_range(0..values.len() as i32) as usize].clone()
                };
                format!("ROW({})::{}", values.join(", "), type_name)
            }
            PgCustomType::Range {
                subtype, values, ..
            } => {
                let value = if values.is_empty() {
                    let values = Self::default_range_values(subtype);
                    values[random.random_range(0..values.len() as i32) as usize].clone()
                } else {
                    values[random.random_range(0..values.len() as i32) as usize].clone()
                };
                format!("{}::{}", value, type_name)
            }
        }
    }

    pub(crate) fn constant_value_str(&self, db: &str, ctx: &MockDbContext) -> Vec<String> {
        let type_name = self.type_name(db, ctx);
        match self {
            PgCustomType::Enum { labels, .. } => labels
                .iter()
                .map(|label| {
                    format!(
                        "{}::{}",
                        single_quote(&Self::escape_sql_string(label)),
                        type_name
                    )
                })
                .collect(),
            PgCustomType::Domain { base, values, .. } => {
                let values = if values.is_empty() {
                    base.constant_value_str(db, ctx)
                } else {
                    values.clone()
                };
                values
                    .into_iter()
                    .map(|value| format!("{}::{}", value, type_name))
                    .collect()
            }
            PgCustomType::Composite { values, .. } => values
                .iter()
                .map(|values| format!("ROW({})::{}", values.join(", "), type_name))
                .collect(),
            PgCustomType::Range {
                subtype, values, ..
            } => {
                let values = if values.is_empty() {
                    Self::default_range_values(subtype)
                } else {
                    values.clone()
                };
                values
                    .into_iter()
                    .map(|value| format!("{}::{}", value, type_name))
                    .collect()
            }
        }
    }

    pub(crate) fn collect_type_ddls(
        &self,
        db: &str,
        ctx: &MockDbContext,
        ddl_by_name: &mut HashMap<String, String>,
        ordered_names: &mut Vec<String>,
    ) {
        match self {
            PgCustomType::Domain { base, .. } => {
                PgType::collect_custom_type_ddls(base, db, ctx, ddl_by_name, ordered_names);
            }
            PgCustomType::Range { subtype, .. } => {
                PgType::collect_custom_type_ddls(subtype, db, ctx, ddl_by_name, ordered_names);
            }
            PgCustomType::Composite { fields, .. } => {
                for field in fields {
                    PgType::collect_custom_type_ddls(
                        &field.col_type,
                        db,
                        ctx,
                        ddl_by_name,
                        ordered_names,
                    );
                }
            }
            PgCustomType::Enum { .. } => {}
        }

        let name = self.name().to_string();
        let ddl = self.ddl_stmt(db, ctx);
        match ddl_by_name.get(&name) {
            Some(existing) if existing == &ddl => {}
            Some(existing) => panic!(
                "conflicting pg custom type definitions for {}: {} vs {}",
                name, existing, ddl
            ),
            None => {
                ordered_names.push(name.clone());
                ddl_by_name.insert(name, ddl);
            }
        }
    }

    fn escape_sql_string(value: &str) -> String {
        value.replace('\'', "''")
    }

    fn default_range_values(subtype: &PgType) -> Vec<String> {
        match subtype {
            PgType::Int4 => vec!["'[1,10)'".to_string(), "'[20,30)'".to_string()],
            PgType::Int8 => vec![
                "'[1000000,2000000)'".to_string(),
                "'[3000000,4000000)'".to_string(),
            ],
            PgType::Numeric => vec!["'[1.25,9.75)'".to_string(), "'[10.5,20.5)'".to_string()],
            PgType::Date => vec![
                "'[2024-01-01,2024-02-01)'".to_string(),
                "'[2024-03-01,2024-04-01)'".to_string(),
            ],
            PgType::Timestamp => vec![
                "'[2024-01-01 00:00:00,2024-01-02 00:00:00)'".to_string(),
                "'[2024-02-01 00:00:00,2024-02-02 00:00:00)'".to_string(),
            ],
            PgType::Timestamptz => vec![
                "'[2024-01-01 00:00:00+00,2024-01-02 00:00:00+00)'".to_string(),
                "'[2024-02-01 00:00:00+00,2024-02-02 00:00:00+00)'".to_string(),
            ],
            _ => vec!["'empty'".to_string()],
        }
    }
}

fn single_quote(value: &str) -> String {
    format!("'{}'", value)
}

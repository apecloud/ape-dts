use anyhow::{ensure, Context, Result};
use serde::de::DeserializeOwned;

use super::{ResourceConfigParser, ResourceKind, RID};
use crate::{config::config_enums::DbType, meta::mssql::MSSQL_DEFAULT_SCHEMA};

const JSON_PREFIX: &str = "json:";

impl ResourceConfigParser {
    pub fn legacy_json_table(db: &str, tb: &str, db_type: &DbType) -> Result<RID> {
        if matches!(db_type, DbType::Mssql) {
            let mut path = vec![db.to_string()];
            let mut tables = Self::paths(tb, db_type)?;
            ensure!(tables.len() == 1, "expected one MSSQL table selector");
            path.extend(tables.remove(0));
            Self::legacy_exact_path(Self::legacy_table(path, db_type)?, db_type)
        } else {
            Ok(RID::new([db, tb]))
        }
    }

    pub fn legacy_table(mut path: Vec<String>, db_type: &DbType) -> Result<Vec<String>> {
        if matches!(db_type, DbType::Mssql) && path.len() == 2 {
            path.insert(1, MSSQL_DEFAULT_SCHEMA.into());
        }
        let kind = if matches!(db_type, DbType::Mongo) {
            ResourceKind::Collection
        } else {
            ResourceKind::Table
        };
        let depth = kind
            .depth(db_type)
            .context("engine does not support table/collection configuration")?;
        ensure!(
            path.len() == depth + 1,
            "invalid legacy {db_type} table path: {path:?}"
        );
        Ok(path)
    }

    pub fn legacy_exact_path(tokens: Vec<String>, db_type: &DbType) -> Result<RID> {
        Ok(RID::new(
            tokens
                .into_iter()
                .map(|token| Ok(Self::quoted_literal(&token, db_type)?.unwrap_or(token)))
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    pub fn legacy_json<T: DeserializeOwned>(value: &str) -> Result<T> {
        let value = value.trim();
        serde_json::from_str(value.strip_prefix(JSON_PREFIX).unwrap_or(value))
            .context("invalid legacy configuration JSON")
    }
}

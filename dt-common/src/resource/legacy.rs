use anyhow::{ensure, Context, Result};

use super::{
    config_parser::{paths, quoted_literal},
    ResourceKind, RID,
};
use crate::config::config_enums::DbType;

pub fn exact_path(tokens: Vec<String>, db_type: &DbType) -> Result<RID> {
    RID::new(
        tokens
            .into_iter()
            .map(|token| Ok(quoted_literal(&token, db_type)?.unwrap_or(token)))
            .collect::<Result<Vec<_>>>()?,
    )
}

pub fn table(mut path: Vec<String>, db_type: &DbType) -> Result<Vec<String>> {
    if matches!(db_type, DbType::Mssql) && path.len() == 2 {
        path.insert(1, "dbo".into());
    }
    let kind = if matches!(db_type, DbType::Mongo) {
        ResourceKind::Collection
    } else {
        ResourceKind::Table
    };
    let depth = super::resource_depth(db_type, kind)
        .context("engine does not support table/collection configuration")?;
    ensure!(
        path.len() == depth + 1,
        "invalid legacy {db_type} table path: {path:?}"
    );
    Ok(path)
}

pub fn json_table(db: &str, tb: &str, db_type: &DbType) -> Result<RID> {
    if matches!(db_type, DbType::Mssql) {
        let mut path = vec![db.to_string()];
        let mut tables = paths(tb, db_type)?;
        ensure!(tables.len() == 1, "expected one MSSQL table selector");
        path.extend(tables.remove(0));
        exact_path(table(path, db_type)?, db_type)
    } else {
        RID::new([db, tb])
    }
}

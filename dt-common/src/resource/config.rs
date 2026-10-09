//! Shared configuration boundaries for the standalone filter and router.

use anyhow::{ensure, Context, Result};
use configparser::ini::Ini;

use super::{resource_levels, ResourceKind, RID};
use crate::{config::config_enums::DbType, utils::sql_util::SqlUtil};

pub const BASE_FILTER_SECTIONS: &[(ResourceKind, &str)] = &[
    (ResourceKind::Database, "filter.database"),
    (ResourceKind::Schema, "filter.schema"),
    (ResourceKind::Table, "filter.table"),
    (ResourceKind::Column, "filter.column"),
    (ResourceKind::Collection, "filter.collection"),
    (ResourceKind::Topic, "filter.topic"),
];

#[derive(Debug, Clone, Default)]
pub(crate) struct BaseConfig {
    pub do_rules: Option<String>,
    pub ignore_rules: Option<String>,
    pub router: Option<String>,
}

pub(crate) fn base_configs(ini: &Ini, db_type: &DbType) -> Result<Vec<BaseConfig>> {
    let levels = resource_levels(db_type);
    let mut result = vec![BaseConfig::default(); levels.len()];
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
        let Some(index) = levels.iter().position(|level| *level == kind) else {
            ensure!(
                fields
                    .values()
                    .all(|v| v.as_deref().unwrap_or("").trim().is_empty()),
                "[{section}] is not supported by {db_type}"
            );
            continue;
        };
        let base = &mut result[index];
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

/// Scan delimiters while preserving quoted/regex tokens. Both endpoint dialects are
/// used to locate boundaries; decoding still validates each token with its own dialect.
pub(crate) fn tokens(input: &str, db_types: &[&DbType]) -> Result<Vec<String>> {
    if input.trim().is_empty() {
        return Ok(Vec::new());
    }
    let pairs: Vec<_> = db_types
        .iter()
        .flat_map(|db| SqlUtil::get_escape_pairs(db))
        .collect();
    let chars: Vec<_> = input.chars().collect();
    let mut out = Vec::new();
    let mut pos = 0;
    loop {
        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }
        let start = pos;
        if chars.get(pos) == Some(&'r') && chars.get(pos + 1) == Some(&'#') {
            pos += 2;
            while pos < chars.len() && chars[pos] != '#' {
                pos += 1;
            }
            ensure!(pos < chars.len(), "unclosed regex in {input}");
            pos += 1;
        } else if let Some(pair) = chars
            .get(pos)
            .and_then(|c| pairs.iter().find(|p| p.left == *c))
        {
            pos += 1;
            loop {
                ensure!(pos < chars.len(), "unclosed identifier in {input}");
                if chars[pos] == pair.right {
                    pos += 1;
                    if pair.inner_escape_mode
                        == crate::utils::sql_util::InnerEscapeMode::DoubleRight
                        && chars.get(pos) == Some(&pair.right)
                    {
                        pos += 1;
                    } else {
                        break;
                    }
                } else {
                    pos += 1;
                }
            }
        } else {
            while pos < chars.len() && !['.', ',', ':'].contains(&chars[pos]) {
                pos += 1;
            }
        }
        let token: String = chars[start..pos].iter().collect::<String>().trim().into();
        ensure!(!token.is_empty(), "empty identifier in {input}");
        out.push(token);
        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos == chars.len() {
            break;
        }
        ensure!(
            ['.', ',', ':'].contains(&chars[pos]),
            "expected delimiter in {input}"
        );
        out.push(chars[pos].to_string());
        pos += 1;
        ensure!(pos < chars.len(), "trailing delimiter in {input}");
    }
    Ok(out)
}

pub(crate) fn path_tokens(tokens: &[String]) -> Result<Vec<String>> {
    ensure!(
        !tokens.is_empty() && tokens.len() % 2 == 1,
        "invalid resource path"
    );
    ensure!(
        tokens.iter().skip(1).step_by(2).all(|t| t == "."),
        "expected dot between resource segments"
    );
    Ok(tokens.iter().step_by(2).cloned().collect())
}

pub(crate) fn paths(input: &str, db_type: &DbType) -> Result<Vec<Vec<String>>> {
    let tokens = tokens(input, &[db_type])?;
    if tokens.is_empty() {
        return Ok(Vec::new());
    }
    tokens
        .split(|token| token == ",")
        .map(path_tokens)
        .collect()
}

pub(crate) fn pairs(
    input: &str,
    src: &DbType,
    dst: &DbType,
) -> Result<Vec<(Vec<String>, Vec<String>)>> {
    let tokens = tokens(input, &[src, dst])?;
    if tokens.is_empty() {
        return Ok(Vec::new());
    }
    tokens
        .split(|token| token == ",")
        .map(|entry| {
            let sides: Vec<_> = entry.split(|token| token == ":").collect();
            ensure!(
                sides.len() == 2,
                "expected source:target mapping in {input}"
            );
            Ok((path_tokens(sides[0])?, path_tokens(sides[1])?))
        })
        .collect()
}

pub(crate) fn literal(token: &str, db_type: &DbType, legacy: bool) -> Result<Option<String>> {
    for pair in SqlUtil::get_escape_pairs(db_type) {
        if token.starts_with(pair.left) {
            ensure!(
                SqlUtil::is_valid_token(token, db_type, &[pair]),
                "invalid quoted {db_type} identifier {token}"
            );
            return Ok(Some(SqlUtil::unescape(token, &pair)));
        }
    }
    // Bare regexes may contain brackets, but names quoted in the other dialect must fail.
    if !legacy && !token.starts_with("r#") {
        ensure!(
            !token.chars().any(|c| ['`', '"', '[', ']'].contains(&c)),
            "invalid unquoted {db_type} identifier {token}"
        );
    }
    Ok(None)
}

pub(crate) fn exact_path(tokens: Vec<String>, db_type: &DbType, legacy: bool) -> Result<RID> {
    RID::new(
        tokens
            .iter()
            .map(|token| {
                if let Some(value) = literal(token, db_type, legacy)? {
                    return Ok(value);
                }
                ensure!(
                    legacy || (!token.contains(['*', '?']) && !token.starts_with("r#")),
                    "router requires exact names; quote literal wildcard names: {token}"
                );
                Ok(token.clone())
            })
            .collect::<Result<Vec<_>>>()?,
    )
}

pub(crate) fn legacy_table(mut path: Vec<String>, db_type: &DbType) -> Result<Vec<String>> {
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
        path.len() == depth,
        "invalid legacy {db_type} table path: {path:?}"
    );
    Ok(path)
}

pub(crate) fn legacy_json_table(db: &str, tb: &str, db_type: &DbType) -> Result<RID> {
    if matches!(db_type, DbType::Mssql) {
        let mut path = vec![db.to_string()];
        let mut tables = paths(tb, db_type)?;
        ensure!(tables.len() == 1, "expected one MSSQL table selector");
        path.extend(tables.remove(0));
        exact_path(legacy_table(path, db_type)?, db_type, true)
    } else {
        RID::new([db, tb])
    }
}

pub(crate) fn json<T: serde::de::DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value.trim().strip_prefix("json:").unwrap_or(value.trim()))
        .context("invalid resource configuration JSON")
}

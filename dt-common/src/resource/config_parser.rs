use anyhow::{ensure, Context, Result};
use serde::de::DeserializeOwned;

use super::{resource_depth, ResourceKind, REGEX_PREFIX, REGEX_SUFFIX, RID};
use crate::{
    config::{config_enums::DbType, filter_config::RULES},
    utils::sql_util::{CharEscapePair, InnerEscapeMode, SqlUtil},
};

pub fn section_kind(name: &str, db_type: &DbType, empty: bool) -> Result<Option<ResourceKind>> {
    let section = format!("{RULES}.{name}");
    let kind = name
        .parse::<ResourceKind>()
        .with_context(|| format!("unknown resource section [{section}]"))?;
    if resource_depth(db_type, kind).is_none() {
        ensure!(empty, "[{section}] is not supported by {db_type}");
        return Ok(None);
    }
    Ok(Some(kind))
}

/// Scan with both endpoint dialects; decode each token with its own dialect later.
fn tokens(input: &str, db_types: &[&DbType]) -> Result<Vec<String>> {
    if input.trim().is_empty() {
        return Ok(Vec::new());
    }
    let pairs: Vec<_> = db_types
        .iter()
        .flat_map(|db| SqlUtil::get_escape_pairs(db))
        .collect();
    let mut out = Vec::new();
    let mut remaining = input;
    loop {
        let (value, rest) = token(remaining, &pairs)?;
        out.push(value.to_owned());
        let Some(delimiter) = rest.chars().next() else {
            break;
        };
        ensure!(
            ['.', ',', ':'].contains(&delimiter),
            "expected delimiter in {input}"
        );
        out.push(delimiter.to_string());
        remaining = &rest[delimiter.len_utf8()..];
        ensure!(!remaining.is_empty(), "trailing delimiter in {input}");
    }
    Ok(out)
}

/// Read one RID segment, leaving the following delimiter unconsumed.
pub fn token<'a>(input: &'a str, pairs: &[CharEscapePair]) -> Result<(&'a str, &'a str)> {
    let input = input.trim_start();
    let end = if let Some(raw) = input.strip_prefix(REGEX_PREFIX) {
        REGEX_PREFIX.len()
            + raw.find(REGEX_SUFFIX).context("unclosed resource regex")?
            + REGEX_SUFFIX.len()
    } else if let Some(pair) = pairs.iter().find(|pair| input.starts_with(pair.left)) {
        let mut chars = input.char_indices().peekable();
        chars.next();
        loop {
            let (index, ch) = chars.next().context("unclosed resource identifier")?;
            if ch == pair.right {
                if pair.inner_escape_mode == InnerEscapeMode::DoubleRight
                    && chars.peek().is_some_and(|(_, next)| *next == pair.right)
                {
                    chars.next();
                } else {
                    break index + ch.len_utf8();
                }
            }
        }
    } else {
        input.find(['.', ',', ':']).unwrap_or(input.len())
    };
    let value = input[..end].trim();
    ensure!(!value.is_empty(), "empty resource identifier");
    Ok((value, input[end..].trim_start()))
}

fn path_tokens(tokens: &[String]) -> Result<Vec<String>> {
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

pub fn paths(input: &str, db_type: &DbType) -> Result<Vec<Vec<String>>> {
    let tokens = tokens(input, &[db_type])?;
    if tokens.is_empty() {
        return Ok(Vec::new());
    }
    tokens
        .split(|token| token == ",")
        .map(path_tokens)
        .collect()
}

pub fn pairs(input: &str, src: &DbType, dst: &DbType) -> Result<Vec<(Vec<String>, Vec<String>)>> {
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

pub fn quoted_literal(token: &str, db_type: &DbType) -> Result<Option<String>> {
    for pair in SqlUtil::get_escape_pairs(db_type) {
        if token.starts_with(pair.left) {
            ensure!(
                SqlUtil::is_valid_token(token, db_type, &[pair]),
                "invalid quoted {db_type} identifier {token}"
            );
            return Ok(Some(SqlUtil::unescape(token, &pair)));
        }
    }
    Ok(None)
}

pub fn literal(token: &str, db_type: &DbType) -> Result<Option<String>> {
    if let Some(value) = quoted_literal(token, db_type)? {
        return Ok(Some(value));
    }
    // Regex brackets are valid; identifier quotes from another dialect are not.
    if !token.starts_with(REGEX_PREFIX) {
        ensure!(
            !token.chars().any(|c| ['`', '"', '[', ']'].contains(&c)),
            "invalid unquoted {db_type} identifier {token}"
        );
    }
    Ok(None)
}

pub fn exact_path(tokens: Vec<String>, db_type: &DbType) -> Result<RID> {
    Ok(RID::new(
        tokens
            .iter()
            .map(|token| {
                if let Some(value) = literal(token, db_type)? {
                    return Ok(value);
                }
                ensure!(
                    !token.contains(['*', '?']) && !token.starts_with(REGEX_PREFIX),
                    "resource path requires exact names; quote literal wildcard names: {token}"
                );
                Ok(token.clone())
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

pub fn json<T: DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value.trim().strip_prefix("json:").unwrap_or(value.trim()))
        .context("invalid resource configuration JSON")
}

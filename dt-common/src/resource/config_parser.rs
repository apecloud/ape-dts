use anyhow::{ensure, Result};
use strum::IntoEnumIterator;

use super::{OPTIONAL_WILDCARD, REGEX_PREFIX, REGEX_SUFFIX, RID, WILDCARD};
use crate::{
    config::{
        config_enums::DbType,
        config_token_parser::{ConfigTokenParser, TokenEscapePair},
    },
    utils::sql_util::SqlUtil,
};

pub const RESOURCE_SEPARATOR: &str = ".";
pub const RULE_SEPARATOR: &str = ",";
pub const MAPPING_SEPARATOR: &str = ":";
pub const RESOURCE_DELIMITERS: &[char] = &['.', ',', ':'];

pub struct ResourceConfigParser;

impl ResourceConfigParser {
    pub fn paths(input: &str, db_type: &DbType) -> Result<Vec<Vec<String>>> {
        let tokens = Self::tokens(input, &[db_type])?;
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        tokens
            .split(|token| token == RULE_SEPARATOR)
            .map(Self::path_tokens)
            .collect()
    }

    pub fn pairs(
        input: &str,
        src: &DbType,
        dst: &DbType,
    ) -> Result<Vec<(Vec<String>, Vec<String>)>> {
        let tokens = Self::tokens(input, &[src, dst])?;
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        tokens
            .split(|token| token == RULE_SEPARATOR)
            .map(|entry| {
                let sides: Vec<_> = entry.split(|token| token == MAPPING_SEPARATOR).collect();
                ensure!(
                    sides.len() == 2,
                    "expected source:target mapping in {input}"
                );
                Ok((Self::path_tokens(sides[0])?, Self::path_tokens(sides[1])?))
            })
            .collect()
    }

    pub fn exact_path(tokens: Vec<String>, db_type: &DbType) -> Result<RID> {
        Ok(RID::new(
            tokens
                .into_iter()
                .map(|token| Self::exact_token(token, db_type))
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    pub fn exact_token(token: String, db_type: &DbType) -> Result<String> {
        if let Some(value) = Self::literal(&token, db_type)? {
            return Ok(value);
        }
        ensure!(
            !token.contains(WILDCARD)
                && !token.contains(OPTIONAL_WILDCARD)
                && !token.starts_with(REGEX_PREFIX),
            "resource path requires exact names; quote literal wildcard names: {token}"
        );
        Ok(token)
    }

    pub fn literal(token: &str, db_type: &DbType) -> Result<Option<String>> {
        if let Some(value) = Self::quoted_literal(token, db_type)? {
            return Ok(Some(value));
        }
        if !token.starts_with(REGEX_PREFIX) {
            ensure!(
                !DbType::iter()
                    .flat_map(|db| SqlUtil::get_escape_pairs(&db))
                    .any(|pair| token.contains([pair.left, pair.right])),
                "invalid unquoted {db_type} identifier {token}"
            );
        }
        Ok(None)
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

    pub fn escape_pairs(db_types: &[&DbType]) -> Vec<TokenEscapePair> {
        db_types
            .iter()
            .flat_map(|db| SqlUtil::get_escape_pairs(db))
            .map(TokenEscapePair::from)
            .chain([TokenEscapePair::from((
                REGEX_PREFIX.into(),
                REGEX_SUFFIX.into(),
            ))])
            .collect()
    }

    /// Scan both dialects; decode tokens using their endpoint dialect.
    fn tokens(input: &str, db_types: &[&DbType]) -> Result<Vec<String>> {
        ConfigTokenParser::tokens(input, RESOURCE_DELIMITERS, &Self::escape_pairs(db_types))
    }

    fn path_tokens(tokens: &[String]) -> Result<Vec<String>> {
        ensure!(
            tokens
                .iter()
                .skip(1)
                .step_by(2)
                .all(|t| t == RESOURCE_SEPARATOR),
            "expected dot between resource segments"
        );
        Ok(tokens.iter().step_by(2).cloned().collect())
    }
}

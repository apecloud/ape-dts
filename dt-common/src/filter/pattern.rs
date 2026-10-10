use std::collections::HashSet;

use anyhow::{ensure, Context, Result};
use regex::Regex;

use crate::{
    config::config_enums::DbType,
    resource::{config, RID},
};

#[derive(Debug, Clone)]
enum SegmentMatcher {
    Literal(String),
    Pattern { regex: Regex, universal: bool },
}

impl SegmentMatcher {
    fn parse(token: &str, db_type: &DbType, legacy: bool) -> Result<Self> {
        if let Some(literal) = config::literal(token, db_type, legacy)? {
            return Ok(Self::Literal(literal));
        }
        let raw = token.strip_prefix("r#").and_then(|s| s.strip_suffix('#'));
        let expression = if let Some(raw) = raw {
            if legacy {
                format!("^{raw}$")
            } else {
                format!("^(?:{raw})$")
            }
        } else if legacy && token.chars().any(|c| "*?+[](){}|^$\\".contains(c)) {
            format!(
                "^{}$",
                token
                    .replace('.', "\\.")
                    .replace('*', ".*")
                    .replace('?', ".?")
            )
        } else if token.contains(['*', '?']) {
            let mut expression = String::from("^");
            for ch in token.chars() {
                match ch {
                    '*' => expression.push_str(".*"),
                    '?' => expression.push_str(".?"),
                    _ => expression.push_str(&regex::escape(&ch.to_string())),
                }
            }
            expression.push('$');
            expression
        } else {
            return Ok(Self::Literal(token.to_string()));
        };
        Ok(Self::Pattern {
            regex: Regex::new(&expression)
                .with_context(|| format!("invalid resource pattern {token}"))?,
            universal: token == "*",
        })
    }

    fn matches(&self, value: &str) -> bool {
        match self {
            Self::Literal(literal) => literal == value,
            Self::Pattern { regex, .. } => regex.is_match(value),
        }
    }

    fn key(&self) -> (bool, &str) {
        match self {
            Self::Literal(value) => (true, value),
            Self::Pattern { regex, .. } => (false, regex.as_str()),
        }
    }
}

/// A configuration pattern, separate from a concrete RID. Each segment matches independently.
#[derive(Debug, Clone)]
pub struct IdPattern {
    segments: Vec<SegmentMatcher>,
}

impl IdPattern {
    pub fn parse(value: &str, db_type: &DbType) -> Result<Self> {
        let paths = config::paths(value, db_type)?;
        ensure!(paths.len() == 1, "expected one resource pattern");
        Self::from_tokens(&paths[0], db_type, false)
    }

    pub fn literal(id: RID) -> Self {
        Self {
            segments: id
                .segments()
                .iter()
                .cloned()
                .map(SegmentMatcher::Literal)
                .collect(),
        }
    }

    pub fn depth(&self) -> usize {
        self.segments.len()
    }

    pub fn matches(&self, id: &RID) -> bool {
        self.depth() == id.len() && self.matches_prefix(id.segments())
    }

    pub(crate) fn from_tokens(tokens: &[String], db_type: &DbType, legacy: bool) -> Result<Self> {
        ensure!(!tokens.is_empty(), "empty resource pattern");
        Ok(Self {
            segments: tokens
                .iter()
                .map(|token| SegmentMatcher::parse(token, db_type, legacy))
                .collect::<Result<_>>()?,
        })
    }

    fn matches_prefix(&self, prefix: &[String]) -> bool {
        prefix.len() <= self.depth()
            && self
                .segments
                .iter()
                .zip(prefix)
                .all(|(segment, name)| segment.matches(name))
    }

    fn covers_descendants(&self, prefix: &[String]) -> bool {
        self.matches_prefix(prefix)
            && self.segments[prefix.len()..].iter().all(|s| {
                matches!(
                    s,
                    SegmentMatcher::Pattern {
                        universal: true,
                        ..
                    }
                )
            })
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MatcherSet {
    exact: HashSet<RID>,
    patterns: Vec<IdPattern>,
}

impl MatcherSet {
    pub fn new(patterns: Vec<IdPattern>) -> Result<Self> {
        let mut result = Self::default();
        for pattern in patterns {
            result.insert(pattern)?;
        }
        Ok(result)
    }

    pub fn insert(&mut self, pattern: IdPattern) -> Result<()> {
        let literal: Option<Vec<_>> = pattern
            .segments
            .iter()
            .map(|s| match s {
                SegmentMatcher::Literal(value) => Some(value.clone()),
                _ => None,
            })
            .collect();
        if let Some(segments) = literal {
            self.exact.insert(RID::new(segments)?);
        } else {
            self.patterns.push(pattern);
        }
        Ok(())
    }

    pub fn matches(&self, id: &RID) -> bool {
        self.exact.contains(id) || self.patterns.iter().any(|pattern| pattern.matches(id))
    }

    pub fn matches_prefix(&self, prefix: &[String]) -> bool {
        self.exact
            .iter()
            .any(|id| id.segments().starts_with(prefix))
            || self.patterns.iter().any(|p| p.matches_prefix(prefix))
    }

    pub fn covers_descendants(&self, prefix: &[String]) -> bool {
        self.patterns.iter().any(|p| p.covers_descendants(prefix))
    }

    pub fn validate_depth(&self, depth: usize) -> Result<()> {
        ensure!(
            self.exact.iter().all(|id| id.len() == depth)
                && self.patterns.iter().all(|p| p.depth() == depth),
            "resource patterns require {depth} segments"
        );
        Ok(())
    }

    pub fn equivalent(&self, other: &Self) -> bool {
        let keys = |set: &Self| {
            set.patterns
                .iter()
                .map(|p| {
                    p.segments
                        .iter()
                        .map(|s| {
                            let (literal, value) = s.key();
                            (literal, value.to_string())
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<HashSet<_>>()
        };
        self.exact == other.exact && keys(self) == keys(other)
    }
}

#[cfg(test)]
mod tests {
    use configparser::ini::Ini;

    use super::*;
    use crate::{
        config::filter_config::{BaseFilterConfig, FilterConfig},
        filter::DtFilter,
        rid,
    };

    fn load_filter(ini: &Ini, db_type: DbType) -> Result<DtFilter> {
        let bases = BaseFilterConfig::from_ini(ini, &db_type)?;
        DtFilter::from_config(&bases, &FilterConfig::from_ini(ini), db_type)
    }

    fn ini(text: &str) -> Ini {
        let mut ini = Ini::new();
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(text.into()).unwrap();
        ini
    }

    fn filter(text: &str) -> DtFilter {
        load_filter(&ini(text), DbType::Mysql).unwrap()
    }

    #[test]
    fn regex_is_segment_anchored_and_quoted_wildcards_are_literal() {
        let f = filter("[filter.table]\ndo=`a.b`.`t*`,r#a|b#.r#order[0-9]+#,unicode.订单\n");
        for id in [
            rid!("a.b", "t*"),
            rid!("a", "order10"),
            rid!("b", "order2"),
            rid!("unicode", "订单"),
        ] {
            assert!(!f.filter(&id).unwrap(), "{id}");
        }
        for id in [
            rid!("a.b", "t123"),
            rid!("ab", "order2"),
            rid!("a", "order2tail"),
        ] {
            assert!(f.filter(&id).unwrap(), "{id}");
        }
        let mssql = load_filter(
            &ini("[filter.table]\ndo=[a.b].[inv]]entory].[ord.er]"),
            DbType::Mssql,
        )
        .unwrap();
        assert!(!mssql.filter(&rid!("a.b", "inv]entory", "ord.er")).unwrap());
    }
}

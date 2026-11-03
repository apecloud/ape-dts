use std::sync::Arc;

use anyhow::{Context, Result};
use dashmap::{mapref::one::Ref, DashMap};
use regex::{Error as RegexError, Regex};

use super::{RMatcher, RID};
use crate::utils::sql_util::{CharEscapePair, SqlUtil};

pub const REGEX_PREFIX: &str = "r#";
pub const REGEX_SUFFIX: &str = "#";
pub const WILDCARD: &str = "*";
pub const OPTIONAL_WILDCARD: &str = "?";

/// SQL quotes are exact; `r#...#` is regex; only bare `*` / `?` are wildcards.
/// Regex anchors wrap the raw expression; use `r#(?:a|b)#` to anchor all alternatives.
#[derive(Debug, Clone, Default)]
pub struct PatternMatcher {
    escape_pairs: Vec<CharEscapePair>,
    regex_cache: Arc<DashMap<String, Regex>>,
}

impl PatternMatcher {
    pub fn new(escape_pairs: Vec<CharEscapePair>) -> Self {
        Self {
            escape_pairs,
            ..Self::default()
        }
    }

    pub fn is_pattern(&self, value: &str) -> bool {
        self.character_escape(value).is_none() && Self::requires_regex(value)
    }

    pub fn is_match_segments(&self, input: &[String], pattern: &[String]) -> bool {
        input.len() == pattern.len()
            && input
                .iter()
                .zip(pattern)
                .all(|(input, pattern)| self.is_match_segment(input, pattern))
    }

    pub fn validate(&self, pattern: &RID) -> Result<()> {
        for segment in pattern.segments() {
            if self.character_escape(segment).is_none() && Self::requires_regex(segment) {
                self.regex(segment)
                    .with_context(|| format!("invalid resource pattern {segment}"))?;
            }
        }
        Ok(())
    }

    fn is_match_segment(&self, input: &str, pattern: &str) -> bool {
        if let Some(pair) = self.character_escape(pattern) {
            return pattern == SqlUtil::escape(input, pair);
        }
        if !Self::requires_regex(pattern) {
            return input == pattern;
        }
        self.regex(pattern).is_ok_and(|regex| regex.is_match(input))
    }

    fn character_escape(&self, pattern: &str) -> Option<&CharEscapePair> {
        self.escape_pairs
            .iter()
            .find(|pair| SqlUtil::is_escaped(pattern, pair))
    }

    fn regex(&self, pattern: &str) -> Result<Ref<'_, String, Regex>, RegexError> {
        if let Some(regex) = self.regex_cache.get(pattern) {
            return Ok(regex);
        }
        let raw = pattern
            .strip_prefix(REGEX_PREFIX)
            .and_then(|inner| inner.strip_suffix(REGEX_SUFFIX));
        let expression = match raw {
            Some(raw) => raw.to_owned(),
            None => regex::escape(pattern)
                .replace(r"\*", ".*")
                .replace(r"\?", ".?"),
        };
        Ok(self
            .regex_cache
            .entry(pattern.to_owned())
            .or_try_insert_with(|| Regex::new(&format!("^{expression}$")))?
            .downgrade())
    }

    fn requires_regex(pattern: &str) -> bool {
        pattern.starts_with(REGEX_PREFIX)
            || pattern.contains(WILDCARD)
            || pattern.contains(OPTIONAL_WILDCARD)
    }
}

impl RMatcher for PatternMatcher {
    fn is_match(&self, input: &RID, pattern: &RID) -> bool {
        self.is_match_segments(input.segments(), pattern.segments())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{config::config_enums::DbType, rid};

    fn matcher(db_type: &DbType) -> PatternMatcher {
        PatternMatcher::new(SqlUtil::get_escape_pairs(db_type))
    }

    #[test]
    fn pattern_matching_respects_each_segment_and_requires_the_same_depth() {
        let matcher: &dyn RMatcher = &PatternMatcher::default();
        for (input, pattern, matches) in [
            (rid!("db", "order10"), rid!("db", "order*"), true),
            (rid!("db", "order10"), rid!("db", "order?"), false),
            (rid!("db", "order10"), rid!("db", "r#order[0-9]+#"), true),
            (
                rid!("db", "preorder10"),
                rid!("db", "r#order[0-9]+#"),
                false,
            ),
            (
                rid!("db", "order10tail"),
                rid!("db", "r#order[0-9]+#"),
                false,
            ),
            (rid!("db", "order"), rid!("db", "r#[#"), false),
            (rid!("db", "order"), rid!("db", "Order"), false),
            (rid!("db", "order10"), rid!("db", "order[0-9]+"), false),
            (rid!("inventory_extra"), rid!("r#inventory|market#"), true),
            (rid!("othermarket"), rid!("r#inventory|market#"), true),
            (rid!("unselected"), rid!("r#inventory|market#"), false),
            (rid!("db", "order[0-9]+"), rid!("db", "order[0-9]+"), true),
            (rid!("a.b", "c"), rid!("a", "b.c"), false),
            (rid!("db", "order"), rid!("*"), false),
            (rid!("db"), rid!("*", "*"), false),
        ] {
            assert_eq!(
                matcher.is_match(&input, &pattern),
                matches,
                "{input} {pattern}"
            );
        }
    }

    #[test]
    fn sql_quotes_protect_patterns_and_decode_doubled_delimiters() {
        for (db_type, input, quoted, other) in [
            (DbType::Mysql, "db*", "`db*`", "db1"),
            (DbType::Mysql, "d`b", "`d``b`", "db"),
            (DbType::Pg, "db?", "\"db?\"", "db1"),
            (DbType::Pg, "d\"b", "\"d\"\"b\"", "db"),
            (DbType::Mssql, "d]b*", "[d]]b*]", "d]b1"),
            (DbType::Mysql, "r#.*#", "`r#.*#`", "orders"),
        ] {
            let matcher = matcher(&db_type);
            let pattern = RID::new([quoted, "order*"]);
            assert!(matcher.is_match(&RID::new([input, "orders"]), &pattern));
            assert!(!matcher.is_match(&RID::new([other, "orders"]), &pattern));
        }
        assert!(!matcher(&DbType::Mssql).is_match(&rid!("a]b"), &rid!("[a]b]")));
        assert!(matcher(&DbType::Mysql).validate(&rid!("`r#[#`")).is_ok());
        assert!(matcher(&DbType::Mysql).is_match(&rid!("`db`"), &rid!("`db`")));
        assert!(!matcher(&DbType::Mysql).is_match(&rid!("`db`"), &rid!("```db```")));
    }

    #[test]
    fn ordinary_metacharacters_match_literally_without_compiling_regexes() {
        let matcher = PatternMatcher::default();
        for name in [
            "plus+name",
            "paren(name)",
            "bracket[12]",
            "brace{2}",
            "caret^name",
            "dollar$name",
            "pipe|name",
            r"back\slash",
        ] {
            assert!(!matcher.is_pattern(name), "{name}");
            matcher.validate(&RID::new([name])).unwrap();
            assert!(matcher.is_match_segment(name, name), "{name}");
            assert!(!matcher.is_match_segment(&format!("{name}_tail"), name));
        }
        assert!(matcher.regex_cache.is_empty());
    }

    #[test]
    fn wildcard_matching_handles_optional_characters_unicode_and_multiple_stars() {
        for (input, expect, matches) in [
            ("", "*", true),
            ("", "?", true),
            ("a", "?", true),
            ("ab", "?", false),
            ("a", "a?*a", false),
            ("aba", "a?*a", true),
            ("abc", "*?*?*c", true),
            ("abcx", "*?*?*c", false),
            ("订单", "订?", true),
            ("订", "订?", true),
            ("订单号", "订?", false),
            ("订单号", "订*", true),
            ("FT.SEARCH", "FT.*", true),
            ("FTxSEARCH", "FT.*", false),
            ("order+10", "order+*", true),
            ("order10", "order+*", false),
            ("bracket[12]", "bracket[12]?", true),
            ("bracket[12]x", "bracket[12]?", true),
            ("bracket1x", "bracket[12]?", false),
            ("pipe|tail", "pipe|*", true),
            ("pipe_tail", "pipe|*", false),
            (r"back\slash_tail", r"back\slash*", true),
            ("backslash_tail", r"back\slash*", false),
            ("a\nb", "a*b", false),
            ("a\nb", "a?b", false),
        ] {
            assert_eq!(
                PatternMatcher::default().is_match_segment(input, expect),
                matches,
                "input={input:?}, expect={expect:?}"
            );
        }
    }

    #[test]
    fn regex_cache_shares_successful_compilations_and_skips_errors() {
        let matcher = PatternMatcher::default();
        assert!(matcher.validate(&rid!("orders", "r#id[0-9]+#")).is_ok());
        assert_eq!(matcher.regex_cache.len(), 1);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let matcher = matcher.clone();
                scope.spawn(move || {
                    for _ in 0..10 {
                        assert!(matcher
                            .is_match(&rid!("orders", "id10"), &rid!("orders", "r#id[0-9]+#")));
                        assert!(matcher.is_match(&rid!("a.b"), &rid!("a.b")));
                        assert!(matcher.is_match(&rid!("orders"), &rid!("order*")));
                    }
                });
            }
        });
        assert_eq!(matcher.regex_cache.len(), 2);
        for _ in 0..2 {
            assert!(matcher.validate(&rid!("r#[#")).is_err());
            assert!(!matcher.is_match(&rid!("a"), &rid!("r#[#")));
        }
        assert_eq!(matcher.regex_cache.len(), 2);
    }
}

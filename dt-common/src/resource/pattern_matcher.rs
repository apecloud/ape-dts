use anyhow::{Context, Result};
use regex::{Error as RegexError, Regex};

use super::{RMatcher, RID};
use crate::utils::sql_util::{CharEscapePair, SqlUtil};

pub const REGEX_PREFIX: &str = "r#";
pub const REGEX_SUFFIX: &str = "#";
pub const WILDCARD: &str = "*";
pub const OPTIONAL_WILDCARD: &str = "?";

/// Legacy matching: SQL quotes are exact; `r#...#` is regex; bare `*` / `?` become `.*` / `.?`.
/// Regex anchors wrap the raw expression; use `r#(?:a|b)#` to anchor all alternatives.
#[derive(Debug, Clone, Default)]
pub struct PatternMatcher {
    escape_pairs: Vec<CharEscapePair>,
}

impl PatternMatcher {
    pub fn new(escape_pairs: Vec<CharEscapePair>) -> Self {
        Self { escape_pairs }
    }

    pub fn is_pattern(&self, value: &str) -> bool {
        self.character_escape(value).is_none()
            && (value.contains(WILDCARD)
                || value.contains(OPTIONAL_WILDCARD)
                || value.starts_with(REGEX_PREFIX))
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
            if self.character_escape(segment).is_none() {
                Self::regex(segment)
                    .with_context(|| format!("invalid resource pattern {segment}"))?;
            }
        }
        Ok(())
    }

    fn is_match_segment(&self, input: &str, pattern: &str) -> bool {
        if let Some(pair) = self.character_escape(pattern) {
            return pattern == SqlUtil::escape(input, pair);
        }
        Self::regex(pattern).is_ok_and(|regex| regex.is_match(input))
    }

    fn character_escape(&self, pattern: &str) -> Option<&CharEscapePair> {
        self.escape_pairs
            .iter()
            .find(|pair| SqlUtil::is_escaped(pattern, pair))
    }

    fn regex(pattern: &str) -> Result<Regex, RegexError> {
        let raw = pattern
            .strip_prefix(REGEX_PREFIX)
            .and_then(|inner| inner.strip_suffix(REGEX_SUFFIX));
        let expression = match raw {
            Some(raw) => raw.to_owned(),
            None => pattern
                .replace('.', "\\.")
                .replace(WILDCARD, ".*")
                .replace(OPTIONAL_WILDCARD, ".?"),
        };
        Regex::new(&format!("^{expression}$"))
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
    use crate::{
        config::config_enums::DbType,
        rid,
        utils::sql_util::{CharEscapePair, InnerEscapeMode},
    };

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
            (rid!("db", "order10"), rid!("db", "order[0-9]+"), true),
            (rid!("db", "order[0-9]+"), rid!("db", "order[0-9]+"), false),
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
        assert!(matcher(&DbType::Mysql).is_match(&rid!("`db`"), &rid!("`db`")));
        assert!(!matcher(&DbType::Mysql).is_match(&rid!("`db`"), &rid!("```db```")));
    }

    #[test]
    fn regex_markers_are_independent_of_identifier_escapes() {
        let quoted = PatternMatcher::new(vec![CharEscapePair::new(
            '`',
            '`',
            InnerEscapeMode::DoubleRight,
        )]);
        for matcher in [&PatternMatcher::default(), &quoted] {
            assert!(matcher.is_match(&rid!("订单10"), &rid!("r#订单[0-9]+#")));
            assert!(!matcher.is_match(&rid!("订单10x"), &rid!("r#订单[0-9]+#")));
            assert!(matcher.validate(&rid!("r#订单[0-9]+#")).is_ok());
            assert!(!matcher.is_match(&rid!("订单"), &rid!("r#[#")));
            assert!(matcher.validate(&rid!("r#[#")).is_err());
        }
        assert!(quoted.is_match(&rid!("r#.*#"), &rid!("`r#.*#`")));
        assert!(!quoted.is_match(&rid!("订单"), &rid!("`r#.*#`")));
        assert!(quoted.validate(&rid!("`r#[#`")).is_ok());
    }

    #[test]
    fn wildcard_matching_handles_optional_characters_unicode_and_multiple_stars() {
        for (input, expect, matches) in [
            ("", "*", true),
            ("", "?", true),
            ("a", "?", true),
            ("ab", "?", false),
            ("a", "?a", true),
            ("aa", "?a", true),
            ("a", "a?", true),
            ("a", "a?*a", false),
            ("aba", "a?*a", true),
            ("abc", "*?*?*c", true),
            ("c", "*?*?*c", true),
            ("abcx", "*?*?*c", false),
            ("ababa", "*aba*ba", true),
            ("aba", "*aba*ba", false),
            ("订单", "订?", true),
            ("订", "订?", true),
            ("订单号", "订?", false),
            ("订单号", "订*", true),
            ("FT.SEARCH", "FT.*", true),
            ("FTxSEARCH", "FT.*", false),
            ("order+10", "order+*", true),
            ("order10", "order+*", true),
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
}

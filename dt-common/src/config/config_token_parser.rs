use anyhow::{bail, ensure, Context, Result};

use super::config_enums::DbType;
use crate::{
    error::DtError,
    utils::sql_util::{CharEscapePair, InnerEscapeMode, SqlUtil},
};

#[derive(Debug, Clone)]
pub enum TokenEscapePair {
    Char(CharEscapePair),
    String((String, String)),
}

impl From<CharEscapePair> for TokenEscapePair {
    fn from(value: CharEscapePair) -> Self {
        Self::Char(value)
    }
}

impl From<(String, String)> for TokenEscapePair {
    fn from(value: (String, String)) -> Self {
        Self::String(value)
    }
}

impl TokenEscapePair {
    pub fn from_char_pairs(char_pairs: Vec<CharEscapePair>) -> Vec<Self> {
        char_pairs.into_iter().map(Self::from).collect()
    }
}

pub struct ConfigTokenParser {}

impl ConfigTokenParser {
    /// Parses tokens while retaining each delimiter as the following array item.
    /// For example, `db.schema,other` becomes `["db", ".", "schema", ",", "other"]`.
    pub fn parse_config_with_delimiters(
        config_str: &str,
        db_type: &DbType,
        delimiters: &[char],
        custom_escape_pairs: Option<&[TokenEscapePair]>,
    ) -> anyhow::Result<Vec<String>> {
        if config_str.is_empty() {
            return Ok(Vec::new());
        }

        let escape_pairs = SqlUtil::get_escape_pairs(db_type);
        let mut token_escape_pairs = TokenEscapePair::from_char_pairs(escape_pairs.clone());
        if let Some(pairs) = custom_escape_pairs {
            token_escape_pairs.extend_from_slice(pairs);
        }
        let tokens = Self::parse_with_delimiters(config_str, delimiters, &token_escape_pairs);
        for token in tokens.iter().step_by(2) {
            if !SqlUtil::is_valid_token(token, db_type, &escape_pairs) {
                bail! {DtError::InvalidConfig(format!(
                    "config error near: {}, try enclose database/table/column with escapes if there are special characters other than letters and numbers",
                    token
                ))}
            }
        }
        Ok(tokens)
    }

    pub fn parse_config(
        config_str: &str,
        db_type: &DbType,
        delimiters: &[char],
        custom_escape_pairs: Option<&[TokenEscapePair]>,
    ) -> anyhow::Result<Vec<String>> {
        if config_str.is_empty() {
            return Ok(Vec::new());
        }

        Ok(Self::parse_config_with_delimiters(
            config_str,
            db_type,
            delimiters,
            custom_escape_pairs,
        )?
        .into_iter()
        .step_by(2)
        .collect())
    }

    pub fn parse(
        config: &str,
        delimiters: &[char],
        escape_pairs: &[TokenEscapePair],
    ) -> Vec<String> {
        Self::parse_with_delimiters(config, delimiters, escape_pairs)
            .into_iter()
            .step_by(2)
            .collect()
    }

    /// Split resource rules, retaining delimiters and rejecting incomplete tokens.
    pub fn tokens(
        config: &str,
        delimiters: &[char],
        escape_pairs: &[TokenEscapePair],
    ) -> Result<Vec<String>> {
        if config.trim().is_empty() {
            return Ok(Vec::new());
        }
        let mut tokens = Vec::new();
        let mut remaining = config;
        loop {
            let (token, rest) = Self::token(remaining, delimiters, escape_pairs)?;
            tokens.push(token.to_owned());
            let Some(delimiter) = rest.chars().next() else {
                break;
            };
            ensure!(
                delimiters.contains(&delimiter),
                "expected delimiter in {config}"
            );
            tokens.push(delimiter.to_string());
            remaining = &rest[delimiter.len_utf8()..];
        }
        Ok(tokens)
    }

    /// Read one token without consuming the following delimiter.
    pub fn token<'a>(
        input: &'a str,
        delimiters: &[char],
        escape_pairs: &[TokenEscapePair],
    ) -> Result<(&'a str, &'a str)> {
        let whitespace = |c: char| c.is_whitespace() && !delimiters.contains(&c);
        let input = input.trim_start_matches(whitespace);
        let end = Self::token_end(input, delimiters, escape_pairs)
            .context("unclosed configuration token")?;
        let token = input[..end].trim();
        ensure!(!token.is_empty(), "empty configuration token");
        Ok((token, input[end..].trim_start_matches(whitespace)))
    }

    fn parse_with_delimiters(
        config: &str,
        delimiters: &[char],
        escape_pairs: &[TokenEscapePair],
    ) -> Vec<String> {
        if config.is_empty() {
            return Vec::new();
        }
        let mut tokens = Vec::new();
        let mut remaining = config;
        loop {
            let input = remaining
                .trim_start_matches(|c: char| c.is_whitespace() && !delimiters.contains(&c));
            // Legacy parsing leaves incomplete tokens for callers to interpret.
            let end = Self::token_end(input, delimiters, escape_pairs).unwrap_or(input.len());
            tokens.push(input[..end].trim().to_owned());
            let rest = input[end..]
                .trim_start_matches(|c: char| c.is_whitespace() && !delimiters.contains(&c));
            let Some(delimiter) = rest.chars().next() else {
                break;
            };
            tokens.push(delimiter.to_string());
            remaining = &rest[delimiter.len_utf8()..];
        }
        tokens
    }

    fn token_end(
        input: &str,
        delimiters: &[char],
        escape_pairs: &[TokenEscapePair],
    ) -> Option<usize> {
        for pair in escape_pairs {
            match pair {
                TokenEscapePair::Char(pair) if input.starts_with(pair.left) => {
                    let mut chars = input.char_indices().peekable();
                    chars.next();
                    while let Some((index, ch)) = chars.next() {
                        if ch == pair.right {
                            if pair.inner_escape_mode == InnerEscapeMode::DoubleRight
                                && chars.peek().is_some_and(|(_, next)| *next == pair.right)
                            {
                                chars.next();
                            } else {
                                return Some(index + ch.len_utf8());
                            }
                        }
                    }
                    return None;
                }
                TokenEscapePair::String((left, right)) if input.starts_with(left) => {
                    return input[left.len()..]
                        .find(right.as_str())
                        .map(|index| left.len() + index + right.len());
                }
                _ => {}
            }
        }
        Some(
            input
                .find(|c| delimiters.contains(&c))
                .unwrap_or(input.len()),
        )
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn shared_scanner_preserves_escapes_and_unicode_boundaries() {
        let pairs = [
            TokenEscapePair::from(CharEscapePair::new('[', ']', InnerEscapeMode::DoubleRight)),
            TokenEscapePair::from(CharEscapePair::new('"', '"', InnerEscapeMode::DoubleRight)),
            TokenEscapePair::from(("r#".into(), "#".into())),
            TokenEscapePair::from(("«".into(), "»»".into())),
        ];
        for (input, delimiters, expected) in [
            (
                " [db]]1] . \"表😀\" : r#orders[,.]+# ",
                &['.', ',', ':'][..],
                vec!["[db]]1]", ".", "\"表😀\"", ":", "r#orders[,.]+#"],
            ),
            (
                "«订单,😀»»,«二»»",
                &[','][..],
                vec!["«订单,😀»»", ",", "«二»»"],
            ),
            (
                "\"first\" \"second\"",
                &[' '][..],
                vec!["\"first\"", " ", "\"second\""],
            ),
        ] {
            assert_eq!(
                ConfigTokenParser::tokens(input, delimiters, &pairs).unwrap(),
                expected
            );
            assert_eq!(
                ConfigTokenParser::parse(input, delimiters, &pairs),
                expected.into_iter().step_by(2).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn resource_tokens_reject_incomplete_rules_while_legacy_keeps_raw_tokens() {
        let pairs = [
            TokenEscapePair::from(CharEscapePair::new('`', '`', InnerEscapeMode::DoubleRight)),
            TokenEscapePair::from(("r#".into(), "#".into())),
        ];
        for input in ["db..t", "db.t,", "`db", "r#db", "`db`tail", "r#db#tail"] {
            assert!(
                ConfigTokenParser::tokens(input, &['.', ',', ':'][..], &pairs).is_err(),
                "{input}"
            );
        }
        for (input, expected) in [
            ("`db", vec!["`db"]),
            ("r#db", vec!["r#db"]),
            ("SET,,DEL,", vec!["SET", "", "DEL", ""]),
        ] {
            assert_eq!(
                ConfigTokenParser::parse(input, &[','][..], &pairs),
                expected
            );
        }
    }

    #[test]
    fn test_parse_mysql_filter_config_tokens() {
        let config = r#"db_1.tb_1,`db.2`.`tb.2`,`db"3`.tb_3,db_4.`tb"4`,db_5.*,`db.6`.*,db_7*.*,`db.8*`.*,*.*,`*`.`*`,r#.*#.r#.?#,`r#.*#`.`r#.?#`"#;
        let delimiters = vec!['.', ','];
        let escape_pairs = vec![
            TokenEscapePair::Char(CharEscapePair::new('`', '`', InnerEscapeMode::DoubleRight)),
            TokenEscapePair::String(("r#".to_string(), '#'.to_string())),
        ];

        let tokens = ConfigTokenParser::parse(config, &delimiters, &escape_pairs);
        assert_eq!(tokens.len(), 24);
        assert_eq!(tokens[0], "db_1");
        assert_eq!(tokens[1], "tb_1");
        assert_eq!(tokens[2], "`db.2`");
        assert_eq!(tokens[3], "`tb.2`");
        assert_eq!(tokens[4], r#"`db"3`"#);
        assert_eq!(tokens[5], "tb_3");
        assert_eq!(tokens[6], "db_4");
        assert_eq!(tokens[7], r#"`tb"4`"#);
        assert_eq!(tokens[8], "db_5");
        assert_eq!(tokens[9], "*");
        assert_eq!(tokens[10], "`db.6`");
        assert_eq!(tokens[11], "*");
        assert_eq!(tokens[12], "db_7*");
        assert_eq!(tokens[13], "*");
        assert_eq!(tokens[14], "`db.8*`");
        assert_eq!(tokens[15], "*");
        assert_eq!(tokens[16], "*");
        assert_eq!(tokens[17], "*");
        assert_eq!(tokens[18], "`*`");
        assert_eq!(tokens[19], "`*`");
        assert_eq!(tokens[20], "r#.*#");
        assert_eq!(tokens[21], "r#.?#");
        assert_eq!(tokens[22], "`r#.*#`");
        assert_eq!(tokens[23], "`r#.?#`");
    }

    #[test]
    fn test_parse_mysql_router_config_tokens() {
        let config = r#"db_1.tb_1:`db.2`.`tb.2`,`db"3`.tb_3:db_4.`tb"4`"#;
        let delimiters = vec!['.', ',', ':'];
        let escape_pairs = vec![TokenEscapePair::Char(CharEscapePair::new(
            '`',
            '`',
            InnerEscapeMode::DoubleRight,
        ))];

        let tokens = ConfigTokenParser::parse(config, &delimiters, &escape_pairs);
        assert_eq!(tokens.len(), 8);
        assert_eq!(tokens[0], "db_1");
        assert_eq!(tokens[1], "tb_1");
        assert_eq!(tokens[2], "`db.2`");
        assert_eq!(tokens[3], "`tb.2`");
        assert_eq!(tokens[4], r#"`db"3`"#);
        assert_eq!(tokens[5], "tb_3");
        assert_eq!(tokens[6], "db_4");
        assert_eq!(tokens[7], r#"`tb"4`"#);
    }

    #[test]
    fn test_parse_pg_filter_config_tokens() {
        let config = r#"db_1.tb_1,"db.2"."tb.2","db`3".tb_3,db_4."tb`4",db_5.*,"db.6".*,db_7*.*,"db.8*".*,*.*,"*"."*",r#.*#.r#.?#,"r#.*#"."r#.?#""#;
        let delimiters = vec!['.', ','];
        let escape_pairs = vec![
            TokenEscapePair::Char(CharEscapePair::new('"', '"', InnerEscapeMode::DoubleRight)),
            TokenEscapePair::String(("r#".to_string(), '#'.to_string())),
        ];

        let tokens = ConfigTokenParser::parse(config, &delimiters, &escape_pairs);
        assert_eq!(tokens.len(), 24);
        assert_eq!(tokens[0], "db_1");
        assert_eq!(tokens[1], "tb_1");
        assert_eq!(tokens[2], r#""db.2""#);
        assert_eq!(tokens[3], r#""tb.2""#);
        assert_eq!(tokens[4], r#""db`3""#);
        assert_eq!(tokens[5], "tb_3");
        assert_eq!(tokens[6], "db_4");
        assert_eq!(tokens[7], r#""tb`4""#);
        assert_eq!(tokens[8], "db_5");
        assert_eq!(tokens[9], "*");
        assert_eq!(tokens[10], r#""db.6""#);
        assert_eq!(tokens[11], "*");
        assert_eq!(tokens[12], "db_7*");
        assert_eq!(tokens[13], "*");
        assert_eq!(tokens[14], r#""db.8*""#);
        assert_eq!(tokens[15], "*");
        assert_eq!(tokens[16], "*");
        assert_eq!(tokens[17], "*");
        assert_eq!(tokens[18], r#""*""#);
        assert_eq!(tokens[19], r#""*""#);
        assert_eq!(tokens[20], "r#.*#");
        assert_eq!(tokens[21], "r#.?#");
        assert_eq!(tokens[22], r#""r#.*#""#);
        assert_eq!(tokens[23], r#""r#.?#""#);
    }

    #[test]
    fn test_parse_pg_router_config_tokens() {
        let config = r#"db_1.tb_1:"db.2"."tb.2","db`3".tb_3:db_4."tb`4""#;
        let delimiters = vec!['.', ',', ':'];
        let escape_pairs = vec![TokenEscapePair::Char(CharEscapePair::new(
            '"',
            '"',
            InnerEscapeMode::DoubleRight,
        ))];

        let tokens = ConfigTokenParser::parse(config, &delimiters, &escape_pairs);
        assert_eq!(tokens.len(), 8);
        assert_eq!(tokens[0], "db_1");
        assert_eq!(tokens[1], "tb_1");
        assert_eq!(tokens[2], r#""db.2""#);
        assert_eq!(tokens[3], r#""tb.2""#);
        assert_eq!(tokens[4], r#""db`3""#);
        assert_eq!(tokens[5], "tb_3");
        assert_eq!(tokens[6], "db_4");
        assert_eq!(tokens[7], r#""tb`4""#);
    }

    #[test]
    fn test_parse_doubled_right_delimiter_tokens() {
        let config = "`schema``name`.`table.name``part`";
        let tokens = ConfigTokenParser::parse_config(config, &DbType::Mysql, &['.'], None).unwrap();

        assert_eq!(tokens, ["`schema``name`", "`table.name``part`"]);

        let config = r#""schema""name"."table.name""part""#;
        let tokens = ConfigTokenParser::parse_config(config, &DbType::Pg, &['.'], None).unwrap();

        assert_eq!(tokens, [r#""schema""name""#, r#""table.name""part""#]);

        let config = "{schema}}name}.{table.name}}part}";
        let tokens = ConfigTokenParser::parse(
            config,
            &['.'],
            &[TokenEscapePair::Char(CharEscapePair::new(
                '{',
                '}',
                InnerEscapeMode::DoubleRight,
            ))],
        );

        assert_eq!(tokens, ["{schema}}name}", "{table.name}}part}"]);

        let config = "[schema]]name].[table.name]]part]";
        let tokens = ConfigTokenParser::parse_config(config, &DbType::Mssql, &['.'], None).unwrap();

        assert_eq!(tokens, ["[schema]]name]", "[table.name]]part]"]);
        assert_eq!(
            SqlUtil::unescape_by_db_type(&tokens[0], &DbType::Mssql),
            "schema]name"
        );
        assert_eq!(
            SqlUtil::unescape_by_db_type(&tokens[1], &DbType::Mssql),
            "table.name]part"
        );
    }

    #[test]
    fn test_parse_config_with_delimiters_preserves_multipart_escapes() {
        let regex_escape_pairs = [TokenEscapePair::String(("r#".into(), "#".into()))];
        let config = "[db,1].[schema.1].[table.1],r#db,.*#.r#schema\\..*#.r#table.*#";

        let tokens = ConfigTokenParser::parse_config_with_delimiters(
            config,
            &DbType::Mssql,
            &[',', '.'],
            Some(&regex_escape_pairs),
        )
        .unwrap();

        assert_eq!(
            tokens,
            [
                "[db,1]",
                ".",
                "[schema.1]",
                ".",
                "[table.1]",
                ",",
                "r#db,.*#",
                ".",
                "r#schema\\..*#",
                ".",
                "r#table.*#"
            ]
        );
        assert_eq!(
            ConfigTokenParser::parse_config(
                config,
                &DbType::Mssql,
                &[',', '.'],
                Some(&regex_escape_pairs),
            )
            .unwrap(),
            tokens.into_iter().step_by(2).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_parse_symmetric_delimiters_without_inner_escape() {
        let escape_pairs = [TokenEscapePair::Char(CharEscapePair::new(
            '"',
            '"',
            InnerEscapeMode::None,
        ))];
        let tokens = ConfigTokenParser::parse(r#""first" "second""#, &[' '], &escape_pairs);

        assert_eq!(tokens, [r#""first""#, r#""second""#]);
    }

    #[test]
    fn test_parse_emoj_config_tokens() {
        let config = r#"SET "set_key_3_  😀" "val_2_  😀""#;
        let delimiters = vec![' '];
        let escape_pairs = vec![TokenEscapePair::Char(CharEscapePair::new(
            '"',
            '"',
            InnerEscapeMode::None,
        ))];
        let tokens = ConfigTokenParser::parse(config, &delimiters, &escape_pairs);
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], "SET");
        assert_eq!(tokens[1], r#""set_key_3_  😀""#);
        assert_eq!(tokens[2], r#""val_2_  😀""#);

        let config = r#"ZADD key 2 val_2_中文 3 "val_3_  😀""#;
        let tokens = ConfigTokenParser::parse(config, &delimiters, &escape_pairs);
        assert_eq!(tokens.len(), 6);
        assert_eq!(tokens[0], "ZADD");
        assert_eq!(tokens[1], "key");
        assert_eq!(tokens[2], "2");
        assert_eq!(tokens[3], "val_2_中文");
        assert_eq!(tokens[4], "3");
        assert_eq!(tokens[5], r#""val_3_  😀""#);
    }
}

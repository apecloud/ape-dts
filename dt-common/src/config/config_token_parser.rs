use anyhow::bail;

use crate::{error::Error, utils::sql_util::SqlUtil};

use super::config_enums::DbType;

const REGEX_ESCAPE_PAIR: (&str, char) = ("r#", '#');

pub struct ConfigTokenParser {}

impl ConfigTokenParser {
    pub fn parse_config(
        config_str: &str,
        db_type: &DbType,
        delimiters: &[char],
    ) -> anyhow::Result<Vec<String>> {
        if config_str.is_empty() {
            return Ok(Vec::new());
        }

        let escape_pairs = SqlUtil::get_escape_pairs(db_type);
        let tokens = Self::parse(config_str, delimiters, &escape_pairs);
        for token in tokens.iter() {
            if !SqlUtil::is_valid_token(token, db_type, &escape_pairs) {
                bail! {Error::ConfigError(format!(
                    "config error near: {}, try enclose database/table/column with escapes if there are special characters other than letters and numbers",
                    token
                ))}
            }
        }
        Ok(tokens)
    }

    pub fn parse(config: &str, delimiters: &[char], escape_pairs: &[(char, char)]) -> Vec<String> {
        let chars: Vec<char> = config.chars().collect();
        let mut start_index = 0;
        let mut tokens = Vec::new();

        if chars.is_empty() {
            return tokens;
        }

        loop {
            let (token, next_index) =
                Self::read_token(&chars, start_index, delimiters, escape_pairs);
            // trim white spaces
            tokens.push(token.trim().to_string());
            // reach the end of chars
            if next_index >= chars.len() {
                break;
            }
            // skip the token_delimiter
            start_index = next_index + 1;
        }

        tokens
    }

    fn read_token(
        chars: &[char],
        start_index: usize,
        delimiters: &[char],
        escape_pairs: &[(char, char)],
    ) -> (String, usize) {
        // read token surrounded by escapes: `db.2`
        for (escape_left, escape_right) in escape_pairs.iter() {
            if chars[start_index] == *escape_left {
                return Self::read_token_with_escape(
                    chars,
                    start_index,
                    (*escape_left, *escape_right),
                );
            } else if Self::match_prefix(chars, start_index, REGEX_ESCAPE_PAIR.0) {
                return Self::read_token_with_regex_escape(chars, start_index);
            }
        }
        Self::read_token_to_delimiter(chars, start_index, delimiters)
    }

    fn match_prefix(chars: &[char], start_index: usize, prefix: &str) -> bool {
        for (i, c) in prefix.chars().enumerate() {
            if start_index + i >= chars.len() {
                return false;
            }
            if chars[start_index + i] != c {
                return false;
            }
        }
        true
    }

    fn read_token_to_delimiter(
        chars: &[char],
        start_index: usize,
        delimiters: &[char],
    ) -> (String, usize) {
        let mut token = String::new();
        let mut read_count = 0;
        for c in chars.iter().skip(start_index) {
            if delimiters.contains(c) {
                break;
            } else {
                token.push(*c);
                read_count += 1;
            }
        }

        let next_index = start_index + read_count;
        (token, next_index)
    }

    fn read_token_with_escape(
        chars: &[char],
        start_index: usize,
        escape_pair: (char, char),
    ) -> (String, usize) {
        let mut start = false;
        let mut token = String::new();
        let mut read_count = 0;
        for c in chars.iter().skip(start_index) {
            if start && *c == escape_pair.1 {
                token.push(*c);
                read_count += 1;
                break;
            }
            if *c == escape_pair.0 {
                start = true;
            }
            if start {
                token.push(*c);
                read_count += 1;
            }
        }

        // when there are emojs in the token, the read_count may be less than token.len(), for example:
        // in chars, 😀 only takes 1 slot, which is '\u{1f600}'
        // in token, 😀 takes 4 slots, which are: 240, 159, 152, 128
        let next_index = start_index + read_count;
        (token, next_index)
    }

    fn read_token_with_regex_escape(chars: &[char], start_index: usize) -> (String, usize) {
        let mut token = String::new();
        let prefix_len = REGEX_ESCAPE_PAIR.0.len();
        let mut read_count = 0;
        for c in chars.iter().skip(start_index) {
            token.push(*c);
            read_count += 1;
            if read_count > prefix_len && *c == REGEX_ESCAPE_PAIR.1 {
                break;
            }
        }

        let next_index = start_index + read_count;
        (token, next_index)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_mysql_filter_config_tokens() {
        let config = r#"db_1.tb_1,`db.2`.`tb.2`,`db"3`.tb_3,db_4.`tb"4`,db_5.*,`db.6`.*,db_7*.*,`db.8*`.*,*.*,`*`.`*`,r#.*#.r#.?#,`r#.*#`.`r#.?#`"#;
        let delimiters = vec!['.', ','];
        let escape_pairs = vec![('`', '`')];

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
        let escape_pairs = vec![('`', '`')];

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
        let config = r#"db_1.tb_1,"db.2"."tb.2","db`3".tb_3,db_4."tb`4",db_5.*,"db.6".*,db_7*.*,"db.8*".*,*.*,"*"."*""#;
        let delimiters = vec!['.', ','];
        let escape_pairs = vec![('"', '"')];

        let tokens = ConfigTokenParser::parse(config, &delimiters, &escape_pairs);
        assert_eq!(tokens.len(), 20);
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
    }

    #[test]
    fn test_parse_pg_router_config_tokens() {
        let config = r#"db_1.tb_1:"db.2"."tb.2","db`3".tb_3:db_4."tb`4""#;
        let delimiters = vec!['.', ',', ':'];
        let escape_pairs = vec![('"', '"')];

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
    fn test_parse_emoj_config_tokens() {
        let config = r#"SET "set_key_3_  😀" "val_2_  😀""#;
        let delimiters = vec![' '];
        let escape_pairs = vec![('"', '"')];
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

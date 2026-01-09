use regex::Regex;

use crate::config::config_enums::DbType;

pub struct SqlUtil {}

pub const MYSQL_ESCAPE: char = '`';
pub const PG_ESCAPE: char = '"';
pub const REDIS_ESCAPE: char = '"';

#[macro_export]
macro_rules! quote_mysql {
    () => {
        ""
    };
    ($s:expr) => {
        // return borrowed str
        format_args!("{}{}{}", MYSQL_ESCAPE, $s, MYSQL_ESCAPE)
    };
}

#[macro_export]
macro_rules! quote_pg {
    () => {
        ""
    };
    ($s:expr) => {
        format_args!("{}{}{}", PG_ESCAPE, $s, PG_ESCAPE)
    };
}

impl SqlUtil {
    pub fn is_escaped(token: &str, escape_pair: &(char, char)) -> bool {
        token.starts_with(escape_pair.0) && token.ends_with(escape_pair.1)
    }

    pub fn escape(token: &str, escape_pair: &(char, char)) -> String {
        if !Self::is_escaped(token, escape_pair) {
            return format!(r#"{}{}{}"#, escape_pair.0, token, escape_pair.1);
        }
        token.to_string()
    }

    pub fn escape_by_db_type(token: &str, db_type: &DbType) -> String {
        let mut result = token.to_string();
        for escape_pair in Self::get_escape_pairs(db_type) {
            result = Self::escape(token, &escape_pair);
        }
        result
    }

    pub fn unescape(token: &str, escape_pair: &(char, char)) -> String {
        if !Self::is_escaped(token, escape_pair) {
            return token.to_string();
        }
        token
            .trim_start_matches(escape_pair.0)
            .trim_end_matches(escape_pair.1)
            .to_string()
    }

    pub fn unescape_by_db_type(token: &str, db_type: &DbType) -> String {
        let mut result = token.to_string();
        for escape_pair in Self::get_escape_pairs(db_type) {
            result = Self::unescape(token, &escape_pair);
        }
        result
    }

    pub fn escape_cols(cols: &Vec<String>, db_type: &DbType) -> Vec<String> {
        let mut escaped_cols = Vec::new();
        for col in cols {
            escaped_cols.push(Self::escape_by_db_type(col, db_type));
        }
        escaped_cols
    }

    pub fn get_escape_pairs(db_type: &DbType) -> Vec<(char, char)> {
        match db_type {
            DbType::Mysql | DbType::ClickHouse | DbType::Foxlake | DbType::StarRocks => {
                vec![(MYSQL_ESCAPE, MYSQL_ESCAPE)]
            }
            DbType::Pg => vec![(PG_ESCAPE, PG_ESCAPE)],
            DbType::Redis => vec![(REDIS_ESCAPE, REDIS_ESCAPE)],
            _ => vec![],
        }
    }

    /// return: (str, is_hex_str)
    pub fn binary_to_str(v: &[u8]) -> (String, bool) {
        if let Ok(str) = String::from_utf8(v.to_owned()) {
            (str, false)
        } else {
            // charsets like: gbk, big5, ujis, euckr
            (hex::encode(v), true)
        }
    }

    pub fn is_valid_token(token: &str, db_type: &DbType, escape_pairs: &[(char, char)]) -> bool {
        let max_token_len = match db_type {
            DbType::Mysql | DbType::Pg => 64,
            // TODO
            _ => i32::MAX,
        } as usize;

        let _is_valid_token = |token: &str, db_type: &DbType| -> bool {
            match db_type {
                DbType::Mysql | DbType::Pg => {
                    let pattern = format!(r"^[a-zA-Z0-9_\?\*\-]{{1,{}}}$", max_token_len);
                    Regex::new(&pattern).unwrap().is_match(token)
                }
                // TODO
                _ => true,
            }
        };

        for escape_pair in escape_pairs.iter() {
            // token is enclosed by escapes
            if Self::is_escaped(token, escape_pair) {
                let unescaped_token = Self::unescape(token, escape_pair);
                return !unescaped_token.contains(escape_pair.0)
                    && !unescaped_token.contains(escape_pair.1)
                    && !unescaped_token.is_empty()
                    && unescaped_token.len() <= max_token_len;
            }
        }
        // token NOT enclosed by escapes
        // is_valid_token(token, db_type)
        // TODO: currently disable token validation since precheck does not support escape, 2023-11-16
        true
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    #[ignore]
    fn test_check_valid_token_without_escapes() {
        let db_type = DbType::Mysql;
        let escape_pairs = vec![];
        assert!(SqlUtil::is_valid_token(
            "my_database",
            &db_type,
            &escape_pairs
        ));
        assert!(SqlUtil::is_valid_token(
            "database1",
            &db_type,
            &escape_pairs
        ));
        assert!(SqlUtil::is_valid_token(
            "_database",
            &db_type,
            &escape_pairs
        ));
        assert!(SqlUtil::is_valid_token("a", &db_type, &escape_pairs));
        assert!(SqlUtil::is_valid_token("*", &db_type, &escape_pairs));
        assert!(SqlUtil::is_valid_token("?", &db_type, &escape_pairs));
        assert!(SqlUtil::is_valid_token("*?", &db_type, &escape_pairs));
        assert!(SqlUtil::is_valid_token("a*b?c", &db_type, &escape_pairs));
        assert!(SqlUtil::is_valid_token(
            "a*b?c-d-e",
            &db_type,
            &escape_pairs
        ));

        // empty
        assert!(!SqlUtil::is_valid_token("", &db_type, &escape_pairs));
        // invalid characters
        assert!(!SqlUtil::is_valid_token(
            "database@",
            &db_type,
            &escape_pairs
        ));
        // too long
        assert!(!SqlUtil::is_valid_token(
            "ttttttttttttttttttttttttttttttttttttttt_this_is_a_really_long_database_name_that_is_over_64_characters",
            &db_type,
            &escape_pairs
        ));
    }

    #[test]
    fn test_check_valid_token_with_escapes() {
        let db_type = DbType::Mysql;
        let escape_pairs = SqlUtil::get_escape_pairs(&DbType::Mysql);
        assert!(SqlUtil::is_valid_token(
            "`my_database`",
            &db_type,
            &escape_pairs
        ));
        assert!(SqlUtil::is_valid_token(
            "`database1`",
            &db_type,
            &escape_pairs
        ));
        assert!(SqlUtil::is_valid_token(
            "`_database`",
            &db_type,
            &escape_pairs
        ));
        assert!(SqlUtil::is_valid_token("`a`", &db_type, &escape_pairs));
        assert!(SqlUtil::is_valid_token("`*`", &db_type, &escape_pairs));
        assert!(SqlUtil::is_valid_token("`?`", &db_type, &escape_pairs));
        assert!(SqlUtil::is_valid_token("`*?`", &db_type, &escape_pairs));
        assert!(SqlUtil::is_valid_token("`a*b?c`", &db_type, &escape_pairs));

        // empty
        assert!(!SqlUtil::is_valid_token("``", &db_type, &escape_pairs));
        // invalid characters can be put between escapes
        assert!(SqlUtil::is_valid_token(
            "`database@`",
            &db_type,
            &escape_pairs
        ));
        // too long
        assert!(!SqlUtil::is_valid_token(
            "`ttttttttttttttttttttttttttttttttttttttt_this_is_a_really_long_database_name_that_is_over_64_characters`",
            &db_type,
            &escape_pairs
        ));
    }
}

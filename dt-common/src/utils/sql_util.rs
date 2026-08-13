use regex::Regex;
use sqlx::{mysql::MySqlRow, ColumnIndex, Row};

use crate::config::config_enums::DbType;
use crate::meta::mysql::mysql_col_type::MysqlColType;

pub struct SqlUtil {}

pub const MYSQL_ESCAPE: char = '`';
pub const PG_ESCAPE: char = '"';
pub const MSSQL_ESCAPE_LEFT: char = '[';
pub const MSSQL_ESCAPE_RIGHT: char = ']';
pub const REDIS_ESCAPE: char = '"';

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InnerEscapeMode {
    None,
    DoubleRight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CharEscapePair {
    pub left: char,
    pub right: char,
    pub inner_escape_mode: InnerEscapeMode,
}

impl CharEscapePair {
    pub const fn new(left: char, right: char, inner_escape_mode: InnerEscapeMode) -> Self {
        Self {
            left,
            right,
            inner_escape_mode,
        }
    }
}

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
    pub fn is_escaped(token: &str, escape_pair: &CharEscapePair) -> bool {
        token.starts_with(escape_pair.left) && token.ends_with(escape_pair.right)
    }

    pub fn escape(token: &str, escape_pair: &CharEscapePair) -> String {
        if Self::is_escaped(token, escape_pair) {
            return token.to_string();
        }
        let inner = Self::escape_inner_right_delimiters(token, escape_pair);
        format!(r#"{}{}{}"#, escape_pair.left, inner, escape_pair.right)
    }

    pub fn escape_by_db_type(token: &str, db_type: &DbType) -> String {
        let mut result = token.to_string();
        for escape_pair in Self::get_escape_pairs(db_type) {
            result = Self::escape(&result, &escape_pair);
        }
        result
    }

    pub fn unescape(token: &str, escape_pair: &CharEscapePair) -> String {
        if !Self::is_escaped(token, escape_pair) {
            return token.to_string();
        }
        let unescaped = token
            .strip_prefix(escape_pair.left)
            .and_then(|token| token.strip_suffix(escape_pair.right))
            .unwrap_or(token);
        Self::unescape_inner_right_delimiters(unescaped, escape_pair)
    }

    pub fn unescape_by_db_type(token: &str, db_type: &DbType) -> String {
        let mut result = token.to_string();
        for escape_pair in Self::get_escape_pairs(db_type) {
            result = Self::unescape(&result, &escape_pair);
        }
        result
    }

    fn escape_inner_right_delimiters(token: &str, escape_pair: &CharEscapePair) -> String {
        match escape_pair.inner_escape_mode {
            InnerEscapeMode::None => token.to_string(),
            InnerEscapeMode::DoubleRight => token.replace(
                escape_pair.right,
                &format!("{}{}", escape_pair.right, escape_pair.right),
            ),
        }
    }

    fn unescape_inner_right_delimiters(token: &str, escape_pair: &CharEscapePair) -> String {
        match escape_pair.inner_escape_mode {
            InnerEscapeMode::None => token.to_string(),
            InnerEscapeMode::DoubleRight => token.replace(
                &format!("{}{}", escape_pair.right, escape_pair.right),
                &escape_pair.right.to_string(),
            ),
        }
    }

    fn has_valid_inner_delimiters(token: &str, escape_pair: &CharEscapePair) -> bool {
        let Some(inner) = token
            .strip_prefix(escape_pair.left)
            .and_then(|token| token.strip_suffix(escape_pair.right))
        else {
            return false;
        };
        if escape_pair.inner_escape_mode == InnerEscapeMode::None {
            return !inner.contains(escape_pair.left) && !inner.contains(escape_pair.right);
        }
        let mut chars = inner.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == escape_pair.right && chars.next_if_eq(&escape_pair.right).is_none() {
                return false;
            }
        }
        true
    }

    pub fn escape_cols(cols: &Vec<String>, db_type: &DbType) -> Vec<String> {
        let mut escaped_cols = Vec::new();
        for col in cols {
            escaped_cols.push(Self::escape_by_db_type(col, db_type));
        }
        escaped_cols
    }

    pub fn mysql_spatial_as_wkb_expr(col: &str, alias: &str) -> String {
        format!("ST_AsBinary({}) AS {}", col, alias)
    }

    pub fn mysql_spatial_from_wkb_hex_expr(hex_value: &str) -> String {
        format!("ST_GeomFromWKB(x'{}')", hex_value)
    }

    pub fn mysql_spatial_from_wkb_placeholder_expr() -> String {
        "ST_GeomFromWKB(?)".to_string()
    }

    pub fn mysql_comparison_placeholder(col_type: &MysqlColType) -> String {
        // https://dev.mysql.com/doc/refman/5.7/en/type-conversion.html
        match col_type {
            MysqlColType::Time { precision } => format!("CAST(? AS TIME({}))", precision),
            _ => "?".to_string(),
        }
    }

    pub fn get_escape_pairs(db_type: &DbType) -> Vec<CharEscapePair> {
        match db_type {
            DbType::Mysql | DbType::ClickHouse | DbType::StarRocks => {
                vec![CharEscapePair::new(
                    MYSQL_ESCAPE,
                    MYSQL_ESCAPE,
                    InnerEscapeMode::DoubleRight,
                )]
            }
            DbType::Pg => vec![CharEscapePair::new(
                PG_ESCAPE,
                PG_ESCAPE,
                InnerEscapeMode::DoubleRight,
            )],
            DbType::Mssql => vec![CharEscapePair::new(
                MSSQL_ESCAPE_LEFT,
                MSSQL_ESCAPE_RIGHT,
                InnerEscapeMode::DoubleRight,
            )],
            DbType::Redis => vec![CharEscapePair::new(
                REDIS_ESCAPE,
                REDIS_ESCAPE,
                InnerEscapeMode::None,
            )],
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

    pub fn try_get_mysql_string<I>(row: &MySqlRow, index: I) -> anyhow::Result<String>
    where
        I: ColumnIndex<MySqlRow> + Copy,
    {
        match row.try_get::<String, _>(index) {
            Ok(value) => Ok(value),
            Err(_) => Ok(String::from_utf8_lossy(&row.try_get::<Vec<u8>, _>(index)?).into_owned()),
        }
    }

    pub fn try_get_mysql_optional_string<I>(
        row: &MySqlRow,
        index: I,
    ) -> anyhow::Result<Option<String>>
    where
        I: ColumnIndex<MySqlRow> + Copy,
    {
        match row.try_get::<Option<String>, _>(index) {
            Ok(value) => Ok(value),
            Err(_) => Ok(row
                .try_get::<Option<Vec<u8>>, _>(index)?
                .map(|value| String::from_utf8_lossy(&value).into_owned())),
        }
    }

    pub fn is_valid_token(token: &str, db_type: &DbType, escape_pairs: &[CharEscapePair]) -> bool {
        let max_token_len = match db_type {
            DbType::Mysql | DbType::Pg => 64,
            DbType::Mssql => 128,
            // TODO
            _ => i32::MAX,
        } as usize;

        let _is_valid_token = |token: &str, db_type: &DbType| -> bool {
            match db_type {
                DbType::Mysql | DbType::Pg => {
                    let pattern = format!(r"^[a-zA-Z0-9_\?\*\-]{{1,{}}}$", max_token_len);
                    Regex::new(&pattern).is_ok_and(|regex| regex.is_match(token))
                }
                // TODO
                _ => true,
            }
        };

        for escape_pair in escape_pairs.iter() {
            // token is enclosed by escapes
            if Self::is_escaped(token, escape_pair) {
                let unescaped_token = Self::unescape(token, escape_pair);
                return Self::has_valid_inner_delimiters(token, escape_pair)
                    && !unescaped_token.is_empty()
                    && unescaped_token.chars().count() <= max_token_len;
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

    #[test]
    fn test_mysql_spatial_exprs() {
        assert_eq!(
            "ST_AsBinary(`geo`) AS `geo`",
            SqlUtil::mysql_spatial_as_wkb_expr("`geo`", "`geo`")
        );
        assert_eq!(
            "ST_GeomFromWKB(x'0101000000000000000000F03F000000000000F03F')",
            SqlUtil::mysql_spatial_from_wkb_hex_expr("0101000000000000000000F03F000000000000F03F")
        );
        assert_eq!(
            "ST_GeomFromWKB(?)",
            SqlUtil::mysql_spatial_from_wkb_placeholder_expr()
        );
    }

    #[test]
    fn test_asymmetric_identifier_escaping() {
        let escape_pair = CharEscapePair::new('{', '}', InnerEscapeMode::DoubleRight);
        assert_eq!(
            "{order}}detail}",
            SqlUtil::escape("order}detail", &escape_pair)
        );
        assert_eq!(
            "order}detail",
            SqlUtil::unescape("{order}}detail}", &escape_pair)
        );
        assert!(SqlUtil::has_valid_inner_delimiters(
            "{order}}detail}",
            &escape_pair
        ));
        assert!(!SqlUtil::has_valid_inner_delimiters(
            "{order}detail}",
            &escape_pair
        ));

        let no_inner_escape = CharEscapePair::new('{', '}', InnerEscapeMode::None);
        assert_eq!(
            "{order}detail}",
            SqlUtil::escape("order}detail", &no_inner_escape)
        );
        assert!(!SqlUtil::has_valid_inner_delimiters(
            "{order}detail}",
            &no_inner_escape
        ));

        assert_eq!(
            "`order``detail`",
            SqlUtil::escape_by_db_type("order`detail", &DbType::Mysql)
        );
        assert_eq!(
            "order`detail",
            SqlUtil::unescape_by_db_type("`order``detail`", &DbType::Mysql)
        );
        assert_eq!(
            r#""order""detail""#,
            SqlUtil::escape_by_db_type(r#"order"detail"#, &DbType::Pg)
        );

        assert_eq!(
            "[order]]detail]",
            SqlUtil::escape_by_db_type("order]detail", &DbType::Mssql)
        );
        assert_eq!(
            "order]detail",
            SqlUtil::unescape_by_db_type("[order]]detail]", &DbType::Mssql)
        );
        assert!(SqlUtil::is_valid_token(
            "[order]]detail]",
            &DbType::Mssql,
            &SqlUtil::get_escape_pairs(&DbType::Mssql),
        ));
        assert!(!SqlUtil::is_valid_token(
            "[order]detail]",
            &DbType::Mssql,
            &SqlUtil::get_escape_pairs(&DbType::Mssql),
        ));
    }
}

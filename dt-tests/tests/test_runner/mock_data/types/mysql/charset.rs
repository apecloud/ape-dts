use serde::{Deserialize, Serialize};

use crate::test_runner::mock_data::constants::Constants;

#[derive(Hash, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlCharAttrs {
    pub length: u16,
    pub charset: String,
    pub collation: String,
}

#[derive(Hash, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlTextAttrs {
    pub charset: String,
    pub collation: String,
}

impl MysqlCharAttrs {
    pub(crate) fn default_with_length(length: u16) -> Self {
        Self {
            length,
            charset: "utf8mb4".to_string(),
            collation: "utf8mb4_0900_ai_ci".to_string(),
        }
    }

    pub fn ddl_suffix(&self) -> String {
        ddl_suffix(&self.charset, &self.collation)
    }

    pub fn normalize_value(&self, value: &str) -> String {
        normalize_value(&self.collation, value)
    }

    pub fn constant_values(&self) -> Vec<String> {
        constant_values(&self.charset, &self.collation)
    }

    pub fn can_be_unique_key(&self) -> bool {
        true
    }
}

impl MysqlTextAttrs {
    pub(crate) fn default() -> Self {
        Self {
            charset: "utf8mb4".to_string(),
            collation: "utf8mb4_0900_ai_ci".to_string(),
        }
    }

    pub fn ddl_suffix(&self) -> String {
        ddl_suffix(&self.charset, &self.collation)
    }

    pub fn normalize_value(&self, value: &str) -> String {
        normalize_value(&self.collation, value)
    }

    pub fn constant_values(&self) -> Vec<String> {
        constant_values(&self.charset, &self.collation)
    }
}

fn ddl_suffix(charset: &str, collation: &str) -> String {
    format!("CHARACTER SET {} COLLATE {}", charset, collation)
}

fn constant_values(charset: &str, collation: &str) -> Vec<String> {
    let values = if supports_4_byte_unicode(charset) {
        Constants::next_str_utf8mb4()
    } else {
        Constants::next_str_utf8mb3()
    };

    values
        .into_iter()
        .map(|value| normalize_value(collation, &value))
        .collect()
}

fn normalize_value(collation: &str, value: &str) -> String {
    if is_case_insensitive_collation(collation) {
        value.to_lowercase()
    } else {
        value.to_string()
    }
}

fn supports_4_byte_unicode(charset: &str) -> bool {
    !(charset.eq_ignore_ascii_case("utf8") || charset.eq_ignore_ascii_case("utf8mb3"))
}

fn is_case_insensitive_collation(collation: &str) -> bool {
    collation.to_ascii_lowercase().ends_with("_ci")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8mb3_constants_include_bmp_symbols_and_exclude_4_byte_emoji() {
        let attrs = MysqlTextAttrs {
            charset: "utf8".to_string(),
            collation: "utf8_bin".to_string(),
        };
        let values = attrs.constant_values();

        assert!(values.contains(&"☃".to_string()));
        assert!(values.contains(&"★".to_string()));
        assert!(!values.contains(&"🔥🚀".to_string()));
    }

    #[test]
    fn test_case_insensitive_collation_normalizes_to_lowercase() {
        let attrs = MysqlTextAttrs {
            charset: "utf8mb4".to_string(),
            collation: "utf8mb4_0900_ai_ci".to_string(),
        };

        assert_eq!(attrs.normalize_value("O'Neil"), "o'neil");
    }
}

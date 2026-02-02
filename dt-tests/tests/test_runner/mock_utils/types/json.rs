use fake::{Fake, Faker};

use crate::test_runner::mock_utils::constants::ConstantValues;
use crate::test_runner::mock_utils::random::{Random, RandomValue};

pub struct Json(pub serde_json::Value);

impl Json {
    pub fn new(value: serde_json::Value) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for Json {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl RandomValue for Json {
    fn next_value(_random: &mut Random) -> String {
        let res: serde_json::Value = Faker.fake();
        Json::new(res).to_string()
    }
}

impl ConstantValues for Json {
    fn next_values() -> Vec<String> {
        [
            // --- 1. Empty Structures ---
            r#"{}"#, // Empty Object
            r#"[]"#, // Empty Array
            // --- 2. Top-level Scalars ---
            // Valid in PG but often crashes parsers expecting Objects/Arrays
            r#"null"#,                   // JSON Null (Distinct from SQL NULL)
            r#"true"#,                   // Boolean True
            r#"false"#,                  // Boolean False
            r#"123456"#,                 // Raw Number
            r#""Raw Top-level String""#, // Raw String (Must include quotes)
            // --- 3. Quotes & Escaping (SQL Injection & Parsing) ---
            r#"{"key": "O'Neil"}"#, // Contains single quote (Common SQL syntax breaker)
            r#"{"key": "Say \"Hi\""}"#, // Contains escaped double quote
            r#"{"path": "C:\\Windows"}"#, // Contains backslash
            // --- 4. Unicode & Encoding ---
            r#"{"cn": "中文测试"}"#,  // CJK Characters (Multi-byte UTF-8)
            r#"{"emoji": "🔥🚀😊"}"#, // Emoji (4-byte characters, requires utf8mb4)
            r#"{"mix": "A\u00A9B"}"#, // Unicode Escape Sequence (e.g., ©)
            // --- 5. Numeric Boundaries ---
            r#"{"max_int": 9007199254740992}"#, // Max Safe Integer (JS limit testing)
            r#"{"tiny": 1.23e-10}"#,            // Scientific Notation (Small values)
            r#"{"PI": 3.14159265358979323846}"#, // High Precision Float
            // --- 6. Structural Complexity ---
            r#"{"a": {"b": {"c": {"d": "deep"}}}}"#, // Deeply Nested Object (Stack overflow testing)
            r#"[1, "mixed", null, {"k":"v"}]"#,      // Heterogeneous Array (Mixed types)
            // --- 7. Type Specific Behavior (json vs jsonb) ---
            // 'json' preserves duplicates; 'jsonb' removes them (keeps last value)
            r#"{"dup": 1, "dup": 2}"#,
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_next_values() {
        let mut random = Random::new(None);
        for _ in 0..5 {
            let json = Json::next_value(&mut random);
            println!("Json: {}", json);
        }
    }

    #[test]
    fn test_all_constant_values() {
        println!("Json constants: {:?}", Json::next_values());
    }
}

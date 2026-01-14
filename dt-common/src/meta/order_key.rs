use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OrderKey {
    Single((String, Option<String>)),
    Composite(Vec<(String, Option<String>)>),
}

impl std::fmt::Display for OrderKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_from_str() {
        let strs = [
            r#"{"single":["id",null]}"#,
            r#"{"single":["id","1"]}"#,
            r#"{"composite":[["id","1"],["name",null]]}"#,
            r#"{"composite":[["id","1"],["name","test"]]}"#,
        ];

        let expected = [
            r#"{"single":["id",null]}"#,
            r#"{"single":["id","1"]}"#,
            r#"{"composite":[["id","1"],["name",null]]}"#,
            r#"{"composite":[["id","1"],["name","test"]]}"#,
        ];

        for (str, expected) in strs.iter().zip(expected.iter()) {
            let order_key: OrderKey = serde_json::from_str(str).unwrap();
            assert_eq!(expected, &serde_json::to_string(&order_key).unwrap());
        }
    }
}

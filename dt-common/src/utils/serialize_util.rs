use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use serde::{Deserialize, Serialize, Serializer};

pub struct SerializeUtil {}

impl SerializeUtil {
    // For use with serde's [serialize_with] attribute
    pub fn ordered_map<S, K, V>(value: &HashMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        K: Ord + Serialize,
        V: Serialize,
        S: Serializer,
    {
        let ordered: BTreeMap<_, _> = value.iter().collect();
        ordered.serialize(serializer)
    }

    pub fn ordered_option_map<S, K, V>(
        value: &Option<HashMap<K, V>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        K: Ord + Serialize,
        V: Serialize,
        S: Serializer,
    {
        match value {
            Some(map) => Self::ordered_map(map, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn serialize_hashmap_to_json<K, V>(value: &HashMap<K, V>) -> anyhow::Result<String>
    where
        K: Ord + Serialize,
        V: Serialize,
    {
        let ordered: BTreeMap<_, _> = value.iter().collect();
        Ok(serde_json::to_string(&ordered)?)
    }

    pub fn deserialize_json_to_hashmap<'a, K, V>(value: &'a str) -> anyhow::Result<HashMap<K, V>>
    where
        K: Ord + Eq + std::hash::Hash + Deserialize<'a>,
        V: Deserialize<'a>,
    {
        let ordered: BTreeMap<_, _> = serde_json::from_str(value)?;
        Ok(ordered.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use serde_json::json;

    #[derive(Serialize)]
    struct TestStruct {
        #[serde(serialize_with = "SerializeUtil::ordered_map")]
        hashmap: HashMap<String, String>,
    }

    #[test]
    fn test_ordered_map() {
        let mut hashmap = HashMap::new();
        hashmap.insert("c".to_string(), "c".to_string());
        hashmap.insert("a".to_string(), "a".to_string());
        hashmap.insert("b".to_string(), "b".to_string());

        let test = TestStruct { hashmap };

        let expected = json!({
            "hashmap": {
                "a": "a",
                "b": "b",
                "c": "c"
            }
        });

        assert_eq!(json!(&test), expected);
    }

    #[test]
    fn test_serialize_hashmap_to_json() {
        let mut map = HashMap::new();
        map.insert("a", 1);
        map.insert("c", 3);
        map.insert("b", 2);
        let json_str = SerializeUtil::serialize_hashmap_to_json(&map).unwrap();
        let expected_json_str = r#"{"a":1,"b":2,"c":3}"#;
        assert_eq!(json_str, expected_json_str);
    }

    #[test]
    fn test_deserialize_json_to_hashmap() {
        let json_str = r#"{"a":1,"c":3,"b":2}"#;
        let map = SerializeUtil::deserialize_json_to_hashmap(json_str).unwrap();
        let expected_map = HashMap::from([("a", 1), ("b", 2), ("c", 3)]);
        assert_eq!(map, expected_map);
    }
}

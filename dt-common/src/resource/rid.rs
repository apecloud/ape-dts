use std::fmt;

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

/// Ordered resource segments; dots within segments are literal.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RID {
    segments: Vec<String>,
}

impl RID {
    pub fn new<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        // Empty db/schema/table components represent absent levels and do not count toward RID depth.
        Self {
            segments: segments
                .into_iter()
                .map(Into::<String>::into)
                .filter(|segment| !segment.is_empty())
                .collect(),
        }
    }

    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn prefix(&self, len: usize) -> Option<Self> {
        (len > 0 && len <= self.len()).then(|| Self {
            segments: self.segments[..len].to_vec(),
        })
    }

    pub fn parent(&self) -> Option<Self> {
        self.len().checked_sub(1).and_then(|len| self.prefix(len))
    }

    pub fn child(&self, name: impl Into<String>) -> Self {
        let mut segments = self.segments.clone();
        segments.push(name.into());
        Self::new(segments)
    }

    pub fn starts_with(&self, prefix: &Self) -> bool {
        self.segments.starts_with(&prefix.segments)
    }

    pub fn replace_prefix(&self, len: usize, target: &Self) -> Result<Self> {
        ensure!(
            len > 0 && len <= self.len(),
            "invalid RID prefix length {len}"
        );
        Ok(Self::new(
            target.segments.iter().chain(&self.segments[len..]).cloned(),
        ))
    }
}

impl fmt::Display for RID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let encoded = serde_json::to_string(&self.segments).map_err(|_| fmt::Error)?;
        f.write_str(&encoded)
    }
}

impl From<&RID> for RID {
    fn from(id: &RID) -> Self {
        id.clone()
    }
}

/// Borrow string expressions to construct a RID without moving them.
#[macro_export]
macro_rules! rid {
    ($($segment:expr),+ $(,)?) => {
        $crate::resource::RID::new([$(::std::convert::AsRef::<str>::as_ref(&($segment))),+])
    };
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::rid;

    #[test]
    fn identity_preserves_boundaries_and_literal_names() {
        let names = [
            rid!("a.b", "c"),
            rid!("a", "b.c"),
            rid!("*", "订单"),
            rid!("a-b", "c"),
            rid!("a", "b-c"),
        ];
        assert_eq!(
            names.iter().cloned().collect::<HashSet<_>>().len(),
            names.len()
        );
        for id in names {
            let encoded = serde_json::to_string(&id).unwrap();
            assert_eq!(serde_json::from_str::<RID>(&encoded).unwrap(), id);
            assert_eq!(id.to_string(), encoded);
        }
        let id: &RID = &rid!("market", "inventory", "order",);
        assert_eq!(id.parent(), Some(rid!("market", "inventory")));
        assert_eq!(id.parent().unwrap().child("order"), *id);
        assert!(id.starts_with(&rid!("market")));
        assert_eq!(id.prefix(0), None);
        assert_eq!(id.prefix(4), None);
        assert_eq!(rid!("market").parent(), None);
    }

    #[test]
    fn construction_omits_empty_segments() {
        for (segments, expected) in [
            (vec![], vec![]),
            (vec![""], vec![]),
            (vec!["", "db", ""], vec!["db"]),
            (vec!["db", "", "table"], vec!["db", "table"]),
            (vec!["", " ", "table", ""], vec![" ", "table"]),
        ] {
            let id = RID::new(segments);
            assert_eq!(id.segments(), expected);
            let json = serde_json::to_string(&id).unwrap();
            assert_eq!(serde_json::from_str::<RID>(&json).unwrap(), id);
        }
        let empty = RID::new(Vec::<String>::new());
        assert!(empty.is_empty());
        assert_eq!(empty.parent(), None);
        assert_eq!(empty.prefix(1), None);
        assert!(empty.replace_prefix(1, &rid!("db")).is_err());
        assert_eq!(empty.child(""), empty);
        assert_eq!(rid!("db").child(""), rid!("db"));
        for json in ["\"db.table\"", "[1]", "null"] {
            assert!(serde_json::from_str::<RID>(json).is_err(), "{json}");
        }
    }

    #[test]
    fn macros_accept_mixed_string_expressions_and_evaluate_them_once() {
        let schema = "inventory".to_owned();
        let mut names = ["orders".to_owned()].into_iter();
        let id = rid!("", schema, names.next().unwrap(),);
        assert_eq!(id.segments(), ["inventory", "orders"]);
        assert_eq!(schema, "inventory");
        assert_eq!(rid!(&schema, "orders"), id);
        assert!(names.next().is_none());
        let borrowed: &RID = &rid!("", schema, format!("{}s", "order"),);
        assert_eq!(*borrowed, id);
        assert_eq!(schema, "inventory");
        let parts = [schema, "orders".to_owned()];
        assert_eq!(rid!(parts[0], parts[1]), id);
        assert_eq!(parts, ["inventory", "orders"]);
    }
}

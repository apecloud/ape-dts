use std::fmt;

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

    pub fn prefix(&self, len: usize) -> Self {
        Self::new(self.segments.iter().take(len).cloned())
    }

    pub fn parent(&self) -> Option<Self> {
        self.len()
            .checked_sub(1)
            .filter(|&len| len > 0)
            .map(|len| self.prefix(len))
    }

    pub fn child(&self, name: impl Into<String>) -> Self {
        let mut segments = self.segments.clone();
        segments.push(name.into());
        Self::new(segments)
    }

    pub fn starts_with(&self, prefix: &Self) -> bool {
        self.segments.starts_with(&prefix.segments)
    }

    pub fn replace_prefix(&self, len: usize, target: &Self) -> Self {
        Self::new(
            target
                .segments
                .iter()
                .chain(self.segments.iter().skip(len))
                .cloned(),
        )
    }

    /// Borrow a segment, returning an empty string when absent.
    pub fn segment(&self, index: usize) -> &str {
        self.segments
            .get(index)
            .map(String::as_str)
            .unwrap_or_default()
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

impl From<RID> for String {
    fn from(id: RID) -> Self {
        id.segments.into_iter().next().unwrap_or_default()
    }
}

impl From<RID> for (String, String) {
    fn from(id: RID) -> Self {
        let mut segments = id.segments.into_iter();
        (
            segments.next().unwrap_or_default(),
            segments.next().unwrap_or_default(),
        )
    }
}

impl From<RID> for (String, String, String) {
    fn from(id: RID) -> Self {
        let mut segments = id.segments.into_iter();
        (
            segments.next().unwrap_or_default(),
            segments.next().unwrap_or_default(),
            segments.next().unwrap_or_default(),
        )
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
    fn borrowed_segments_support_variable_depths_and_missing_levels() {
        for names in [
            vec![],
            vec!["orders"],
            vec!["shop", "orders"],
            vec!["db", "shop", "orders"],
            vec!["server", "db", "shop", "orders"],
        ] {
            let id = RID::new(names.clone());
            for (index, expected) in names.into_iter().enumerate() {
                assert_eq!(id.segment(index), expected);
            }
            for index in [id.len(), usize::MAX] {
                assert_eq!(id.segment(index), "");
            }
        }
    }

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
        assert!(id.prefix(0).is_empty());
        assert_eq!(id.prefix(4), *id);
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
        assert_eq!(empty.prefix(1), empty);
        assert_eq!(empty.replace_prefix(1, &rid!("db")), rid!("db"));
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

    #[test]
    fn owned_conversions_move_strings_in_order_and_default_missing_segments() {
        for names in [
            vec![],
            vec!["inventory"],
            vec!["inventory", "orders"],
            vec!["market", "inventory", "orders"],
            vec!["market", "inventory", "orders", "id"],
        ] {
            let expected = |index| names.get(index).copied().unwrap_or_default();

            let id = RID::new(names.clone());
            let first_ptr = id.segments().first().map(|name| name.as_ptr());
            let first: String = id.into();
            assert_eq!(first, expected(0));
            if let Some(ptr) = first_ptr {
                assert_eq!(first.as_ptr(), ptr);
            }

            let id = RID::new(names.clone());
            let pointers: Vec<_> = id.segments().iter().map(|name| name.as_ptr()).collect();
            let (first, second): (String, String) = id.into();
            assert_eq!(
                (first.as_str(), second.as_str()),
                (expected(0), expected(1))
            );
            for (name, ptr) in [&first, &second].into_iter().zip(&pointers) {
                assert_eq!(name.as_ptr(), *ptr);
            }

            let id = RID::new(names.clone());
            let pointers: Vec<_> = id.segments().iter().map(|name| name.as_ptr()).collect();
            let (first, second, third): (String, String, String) = id.into();
            assert_eq!(
                (first.as_str(), second.as_str(), third.as_str()),
                (expected(0), expected(1), expected(2))
            );
            for (name, ptr) in [&first, &second, &third].into_iter().zip(&pointers) {
                assert_eq!(name.as_ptr(), *ptr);
            }
        }
    }
}

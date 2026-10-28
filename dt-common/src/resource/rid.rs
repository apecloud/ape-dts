use std::{array, fmt};

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
        // Empty segments represent absent resource levels.
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

    pub fn segment(&self, index: usize) -> &str {
        self.segments
            .get(index)
            .map(String::as_str)
            .unwrap_or_default()
    }

    /// Left-align; pad or truncate on the right.
    pub fn into_left<const N: usize>(self) -> [String; N] {
        let mut segments = self.segments.into_iter();
        array::from_fn(|_| segments.next().unwrap_or_default())
    }

    /// Right-align; pad or truncate on the left.
    pub fn into_right<const N: usize>(self) -> [String; N] {
        let mut segments = self.segments.into_iter().rev();
        let mut parts = array::from_fn(|_| segments.next().unwrap_or_default());
        parts.reverse();
        parts
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
        let [segment] = id.into_right();
        segment
    }
}

impl From<RID> for (String, String) {
    fn from(id: RID) -> Self {
        id.into_right().into()
    }
}

impl From<RID> for (String, String, String) {
    fn from(id: RID) -> Self {
        id.into_right().into()
    }
}

/// Borrow each expression once; preserve ownership of inputs.
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
        let names = [rid!("a.b", "c"), rid!("a", "b.c"), rid!("*", "订单")];
        assert_eq!(
            names.iter().cloned().collect::<HashSet<_>>().len(),
            names.len()
        );
        for id in names {
            let encoded = serde_json::to_string(&id).unwrap();
            assert_eq!(serde_json::from_str::<RID>(&encoded).unwrap(), id);
            assert_eq!(id.to_string(), encoded);
        }
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
        let parts = [schema, "orders".to_owned()];
        assert_eq!(rid!(parts[0], parts[1]), id);
        assert_eq!(parts, ["inventory", "orders"]);
    }

    #[test]
    fn alignment_moves_strings_and_pads_or_truncates() {
        for (segments, left, right) in [
            (vec![], ["", "", ""], ["", "", ""]),
            (vec!["t"], ["t", "", ""], ["", "", "t"]),
            (vec!["s", "t"], ["s", "t", ""], ["", "s", "t"]),
            (vec!["db", "s", "t"], ["db", "s", "t"], ["db", "s", "t"]),
            (
                vec!["db", "s", "t", "id"],
                ["db", "s", "t"],
                ["s", "t", "id"],
            ),
        ] {
            let id = RID::new(segments.clone());
            let pointers: Vec<_> = id.segments().iter().map(|s| s.as_ptr()).collect();
            let (a, b, c): (String, String, String) = id.into_left().into();
            let aligned = [a, b, c];
            assert_eq!(aligned, left);
            for (value, pointer) in aligned.iter().zip(pointers) {
                assert_eq!(value.as_ptr(), pointer);
            }

            let id = RID::new(segments.clone());
            let pointers: Vec<_> = id.segments().iter().map(|s| s.as_ptr()).collect();
            let (a, b, c): (String, String, String) = id.into();
            let aligned = [a, b, c];
            assert_eq!(aligned, right);
            for (value, pointer) in aligned.iter().rev().zip(pointers.iter().rev()) {
                assert_eq!(value.as_ptr(), *pointer);
            }
            let (schema, table): (String, String) = RID::new(segments.clone()).into();
            assert_eq!([schema, table], right[1..]);
            let table: String = RID::new(segments).into();
            assert_eq!(table, right[2]);
        }
    }
}

use std::fmt;

use anyhow::{ensure, Result};
use serde::{Deserialize, Deserializer, Serialize};

use super::resource_kind;
use crate::config::config_enums::DbType;

/// A non-empty path of non-empty, unescaped names. Dots inside a name are literal.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct RID {
    segments: Vec<String>,
}

impl RID {
    pub fn new<I, S>(segments: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let segments: Vec<String> = segments.into_iter().map(Into::into).collect();
        ensure!(
            !segments.is_empty(),
            "RID must contain at least one segment"
        );
        ensure!(
            segments.iter().all(|s| !s.is_empty()),
            "RID segments must not be empty"
        );
        Ok(Self { segments })
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
        self.prefix(self.len() - 1)
    }

    pub fn child(&self, name: impl Into<String>) -> Result<Self> {
        let mut segments = self.segments.clone();
        segments.push(name.into());
        Self::new(segments)
    }

    pub fn starts_with(&self, prefix: &Self) -> bool {
        self.segments.starts_with(&prefix.segments)
    }

    pub(crate) fn replace_prefix(&self, len: usize, target: &Self) -> Result<Self> {
        ensure!(
            len > 0 && len <= self.len(),
            "invalid RID prefix length {len}"
        );
        Self::new(target.segments.iter().chain(&self.segments[len..]).cloned())
    }
}

impl<'de> Deserialize<'de> for RID {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let segments = Vec::<String>::deserialize(deserializer)?;
        Self::new(segments).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for RID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // JSON preserves segment boundaries; this is not an SQL renderer.
        let encoded = serde_json::to_string(&self.segments).map_err(|_| fmt::Error)?;
        f.write_str(&encoded)
    }
}

/// Engine identifiers must validate their supported engine, resource kind and required fields.
/// `validate_rid` checks kind/depth; additional engine-specific constraints belong in the adapter.
pub trait ToRid {
    fn to_rid(&self, db_type: &DbType) -> Result<RID>;
}

/// Reconstruct a concrete identifier in the supplied endpoint's context, validating its kind.
pub trait FromRid: Sized {
    fn from_rid(id: &RID, db_type: &DbType) -> Result<Self>;
}

// Direct RID inputs make the isolated modules usable before engine adapters are introduced.
impl ToRid for RID {
    fn to_rid(&self, db_type: &DbType) -> Result<RID> {
        resource_kind(db_type, self)?;
        Ok(self.clone())
    }
}

impl FromRid for RID {
    fn from_rid(id: &RID, db_type: &DbType) -> Result<Self> {
        id.to_rid(db_type)
    }
}

/// Construct an RID from non-empty string literals; dynamic input uses `RID::new`.
#[macro_export]
macro_rules! rid {
    ($($segment:literal),+ $(,)?) => {{
        const _: () = { $(assert!(!$segment.is_empty(), "RID segments must not be empty");)+ };
        $crate::resource::RID::new([$($segment),+]).expect("validated RID literals")
    }};
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
        let id = rid!("market", "inventory", "order");
        assert_eq!(id.parent(), Some(rid!("market", "inventory")));
        assert_eq!(id.parent().unwrap().child("order").unwrap(), id);
        assert!(id.starts_with(&rid!("market")));
        assert_eq!(id.prefix(0), None);
        assert_eq!(id.prefix(4), None);
        assert_eq!(rid!("market").parent(), None);
    }

    #[test]
    fn construction_and_deserialization_enforce_non_empty_segments() {
        for json in ["[]", "[\"\"]", "[\"db\",\"\"]", "\"db.table\"", "[1]"] {
            assert!(serde_json::from_str::<RID>(json).is_err(), "{json}");
        }
        assert!(RID::new(Vec::<String>::new()).is_err());
        assert!(rid!("db").child("").is_err());
    }
}

use serde::{Deserialize, Serialize};

use super::RID;

/// Table names are right-aligned: two segments fill schema_name and table_name.
/// Missing components are empty strings; punctuation within names is literal.
///
/// ```
/// use dt_common::{resource::{TableId, RID}, table_id, table_id_ref};
///
/// let schema = String::from("inventory");
/// let table = table_id!(schema, "orders");
/// let borrowed = table_id_ref!(schema, "orders");
/// assert_eq!(table.as_ref(), borrowed);
/// assert_eq!(table.db_name(), "");
/// assert_eq!(table.schema_name(), schema);
/// let rid = RID::from(borrowed);
/// assert_eq!(TableId::from(&rid), table);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableId {
    #[serde(rename = "db")]
    db_name: String,
    #[serde(rename = "schema")]
    schema_name: String,
    #[serde(rename = "table")]
    table_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableIdRef<'a> {
    #[serde(rename = "db")]
    db_name: &'a str,
    #[serde(rename = "schema")]
    schema_name: &'a str,
    #[serde(rename = "table")]
    table_name: &'a str,
}

impl TableId {
    pub fn new(
        db_name: impl Into<String>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            db_name: db_name.into(),
            schema_name: schema_name.into(),
            table_name: table_name.into(),
        }
    }

    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn as_ref(&self) -> TableIdRef<'_> {
        TableIdRef::new(&self.db_name, &self.schema_name, &self.table_name)
    }
}

impl<'a> TableIdRef<'a> {
    pub fn new(db_name: &'a str, schema_name: &'a str, table_name: &'a str) -> Self {
        Self {
            db_name,
            schema_name,
            table_name,
        }
    }

    pub fn db_name(&self) -> &'a str {
        self.db_name
    }

    pub fn schema_name(&self) -> &'a str {
        self.schema_name
    }

    pub fn table_name(&self) -> &'a str {
        self.table_name
    }

    pub fn to_owned(self) -> TableId {
        self.into()
    }
}

impl<'a> From<&'a TableId> for TableIdRef<'a> {
    fn from(id: &'a TableId) -> Self {
        id.as_ref()
    }
}

impl From<TableIdRef<'_>> for TableId {
    fn from(id: TableIdRef<'_>) -> Self {
        Self::new(id.db_name, id.schema_name, id.table_name)
    }
}

impl From<&TableIdRef<'_>> for TableId {
    fn from(id: &TableIdRef<'_>) -> Self {
        (*id).into()
    }
}

impl From<TableId> for RID {
    fn from(id: TableId) -> Self {
        Self::new([id.db_name, id.schema_name, id.table_name])
    }
}

impl From<&TableId> for RID {
    fn from(id: &TableId) -> Self {
        id.as_ref().into()
    }
}

impl From<TableIdRef<'_>> for RID {
    fn from(id: TableIdRef<'_>) -> Self {
        Self::new([id.db_name, id.schema_name, id.table_name])
    }
}

impl From<&TableIdRef<'_>> for RID {
    fn from(id: &TableIdRef<'_>) -> Self {
        (*id).into()
    }
}

impl<'a> From<&'a RID> for TableIdRef<'a> {
    /// Right-align up to three trailing segments of a table RID.
    fn from(id: &'a RID) -> Self {
        let mut segments = id.segments().iter().rev().map(String::as_str);
        let table_name = segments.next().unwrap_or_default();
        let schema_name = segments.next().unwrap_or_default();
        let db_name = segments.next().unwrap_or_default();
        Self::new(db_name, schema_name, table_name)
    }
}

impl From<&RID> for TableId {
    fn from(id: &RID) -> Self {
        TableIdRef::from(id).to_owned()
    }
}

/// Construct an owned table identifier without moving the name expressions.
#[macro_export]
macro_rules! table_id {
    ($($segment:expr),+ $(,)?) => {
        $crate::table_id_ref!($($segment),+).to_owned()
    };
}

/// Borrow one to three names, filling table, schema, then database from the right.
#[macro_export]
macro_rules! table_id_ref {
    ($table:expr $(,)?) => {
        $crate::resource::TableIdRef::new("", "", &($table))
    };
    ($schema:expr, $table:expr $(,)?) => {
        $crate::resource::TableIdRef::new("", &($schema), &($table))
    };
    ($db:expr, $schema:expr, $table:expr $(,)?) => {
        $crate::resource::TableIdRef::new(&($db), &($schema), &($table))
    };
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::{config::config_enums::DbType, filter::DtFilter, rid, router::DtRouter};

    #[test]
    fn construction_and_conversions_align_names_from_the_right() {
        for (owned, borrowed, expected) in [
            (
                table_id!("orders"),
                table_id_ref!("orders"),
                ["", "", "orders"],
            ),
            (
                table_id!("inventory", "orders",),
                table_id_ref!("inventory", "orders",),
                ["", "inventory", "orders"],
            ),
            (
                table_id!("market", "inventory", "orders"),
                table_id_ref!("market", "inventory", "orders"),
                ["market", "inventory", "orders"],
            ),
            (table_id!(""), table_id_ref!(""), ["", "", ""]),
        ] {
            assert_eq!(owned, TableId::new(expected[0], expected[1], expected[2]));
            assert_eq!(owned.as_ref(), borrowed);
            assert_eq!(TableIdRef::from(&owned), borrowed);
            assert_eq!(
                [owned.db_name(), owned.schema_name(), owned.table_name()],
                expected
            );
            assert_eq!(
                [
                    borrowed.db_name(),
                    borrowed.schema_name(),
                    borrowed.table_name()
                ],
                expected
            );
            assert_eq!(TableId::from(borrowed), owned);
            assert_eq!(TableId::from(&borrowed), owned);

            let json = serde_json::json!({
                "db": expected[0],
                "schema": expected[1],
                "table": expected[2],
            });
            assert_eq!(serde_json::to_value(&owned).unwrap(), json);
            assert_eq!(serde_json::to_value(borrowed).unwrap(), json);
            let encoded = json.to_string();
            assert_eq!(serde_json::from_str::<TableId>(&encoded).unwrap(), owned);
            assert_eq!(
                serde_json::from_str::<TableIdRef<'_>>(&encoded).unwrap(),
                borrowed
            );

            let rid = RID::new(expected);
            assert_eq!(RID::from(&owned), rid);
            assert_eq!(RID::from(borrowed), rid);
            assert_eq!(RID::from(&borrowed), rid);
            assert_eq!(TableId::from(&rid), owned);
            assert_eq!(TableIdRef::from(&rid), borrowed);
            assert_eq!(RID::from(owned), rid);
        }
    }

    #[test]
    fn identities_preserve_literal_names_and_component_boundaries() {
        let ids = [
            table_id!("a.b", "c"),
            table_id!("a", "b.c"),
            table_id!("a-b", "c"),
            table_id!("a", "b-c"),
            table_id!("*", "订单"),
            table_id!("`db`", "r#.*#"),
            table_id!(" ", "Orders"),
            table_id!(" ", "orders"),
        ];
        assert_eq!(ids.iter().collect::<HashSet<_>>().len(), ids.len());
        for id in ids {
            assert_eq!(TableId::from(&RID::from(&id)), id);
        }

        // Named slots stay intact; RID construction omits empty components.
        let id = TableId::new("market", "", "orders");
        assert_eq!(id.db_name(), "market");
        assert_eq!(id.schema_name(), "");
        assert_eq!(RID::from(id), rid!("market", "orders"));
    }

    #[test]
    fn macros_borrow_strings_and_evaluate_expressions_once() {
        let names = [
            "market".to_owned(),
            "inventory".to_owned(),
            "orders".to_owned(),
        ];
        let mut parts = names.iter();
        let owned = table_id!(
            parts.next().unwrap(),
            parts.next().unwrap(),
            parts.next().unwrap(),
        );
        assert!(parts.next().is_none());
        let mut parts = names.iter();
        let borrowed = table_id_ref!(
            parts.next().unwrap(),
            parts.next().unwrap(),
            parts.next().unwrap(),
        );
        assert!(parts.next().is_none());
        assert_eq!(owned.as_ref(), borrowed);
        assert_eq!(borrowed.db_name().as_ptr(), names[0].as_ptr());
        assert_eq!(borrowed.schema_name().as_ptr(), names[1].as_ptr());
        assert_eq!(borrowed.table_name().as_ptr(), names[2].as_ptr());
        assert_eq!(
            table_id_ref!(names[0], names[1].as_str(), &names[2]),
            borrowed
        );
        assert_eq!(table_id!(names[0], names[1].as_str(), &names[2]), owned);
        assert_eq!(
            table_id!(names[0], names[1], format!("{}s", "order")),
            owned
        );
        assert_eq!(names, ["market", "inventory", "orders"]);
        let rid = RID::from(owned);
        assert_eq!(
            TableIdRef::from(&rid).table_name().as_ptr(),
            rid.segments()[2].as_ptr()
        );
    }

    #[test]
    fn table_ids_work_with_resource_filter_and_router() {
        for (src_type, dst_type, source, target) in [
            (
                DbType::Mysql,
                DbType::Pg,
                table_id!("inventory", "orders"),
                table_id!("archive", "orders"),
            ),
            (
                DbType::Pg,
                DbType::Mssql,
                table_id!("inventory", "orders"),
                table_id!("market", "archive", "orders"),
            ),
        ] {
            let mut filter = DtFilter::new(src_type.clone());
            filter.add_do(&source).unwrap();
            assert!(!filter.filter(source.clone()).unwrap());
            assert!(!filter.filter(&source).unwrap());
            assert!(!filter.filter(source.as_ref()).unwrap());
            assert!(filter.filter(rid!("inventory", "events")).unwrap());

            let source_rid = RID::from(&source);
            let target_rid = RID::from(&target);
            let router = DtRouter::new(
                src_type,
                dst_type,
                [(source_rid.clone(), target_rid.clone())],
            )
            .unwrap();
            let routed: RID = router.forward(&source_rid).unwrap();
            assert_eq!(TableId::from(&routed), target);
            let restored: RID = router
                .reverse_route_in(&target_rid, &HashSet::from([source_rid]))
                .unwrap();
            assert_eq!(TableId::from(&restored), source);
        }
    }
}

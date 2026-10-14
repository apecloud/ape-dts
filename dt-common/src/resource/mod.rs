//! Engine-independent resource identities.
//!
//! ```
//! use dt_common::{config::config_enums::DbType, filter::DtFilter, resource::RID, rid, router::DtRouter};
//!
//! let schema = "inventory".to_owned();
//! let table = &rid!(schema, "order");
//! let mut filter = DtFilter::new(DbType::Pg);
//! filter.add_do(rid!(schema))?;
//! assert!(!filter.filter(table)?); // true means excluded
//!
//! let router = DtRouter::new(DbType::Pg, DbType::Mssql,
//!     [(rid!("inventory"), rid!("market", "inventory"))])?;
//! let target = router.forward::<RID>(table)?;
//! assert_eq!(target, rid!("market", "inventory", "order"));
//! assert_eq!(router.reverse::<RID>(&target)?, *table);
//! # Ok::<(), anyhow::Error>(())
//! ```

pub mod config_parser;
mod exact_matcher;
pub mod legacy;
mod pattern_matcher;
mod resource_kind;
mod rid;
mod rmatcher;
mod table_id;

pub use exact_matcher::ExactMatcher;
pub use pattern_matcher::PatternMatcher;
pub use pattern_matcher::{REGEX_PREFIX, REGEX_SUFFIX};
pub use resource_kind::{
    resource_depth, resource_kind, resource_levels, validate_rid, ResourceKind,
};
pub use rid::RID;
pub use rmatcher::RMatcher;
pub use table_id::{TableId, TableIdRef};

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

mod config_parser;
mod exact_matcher;
mod legacy;
mod pattern_matcher;
mod resource_kind;
mod rid;
mod rmatcher;

pub use config_parser::{
    ResourceConfigParser, MAPPING_SEPARATOR, RESOURCE_DELIMITERS, RESOURCE_SEPARATOR,
    RULE_SEPARATOR,
};
pub use exact_matcher::ExactMatcher;
pub use pattern_matcher::PatternMatcher;
pub use pattern_matcher::{OPTIONAL_WILDCARD, REGEX_PREFIX, REGEX_SUFFIX, WILDCARD};
pub use resource_kind::ResourceKind;
pub use rid::RID;
pub use rmatcher::RMatcher;

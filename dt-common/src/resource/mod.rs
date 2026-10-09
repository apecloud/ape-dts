//! Engine-independent resource identities. These modules do not change the pipeline's identities.
//!
//! ```
//! use dt_common::{config::config_enums::DbType, filter::DtFilter, rid, router::Router};
//!
//! let table = rid!("inventory", "order");
//! let mut filter = DtFilter::new(DbType::Pg);
//! filter.add_do(&rid!("inventory"))?;
//! assert!(!filter.filter(&table)?); // true means excluded
//!
//! let router = Router::new(DbType::Pg, DbType::Mssql,
//!     [(rid!("inventory"), rid!("market", "inventory"))])?;
//! let target = router.route(&table)?;
//! assert_eq!(target, rid!("market", "inventory", "order"));
//! assert_eq!(router.reverse_route(&target)?, table);
//! # Ok::<(), anyhow::Error>(())
//! ```

pub(crate) mod config;
mod kind;
mod rid;

pub use kind::{resource_depth, resource_kind, resource_levels, validate_rid, ResourceKind};
pub use rid::{FromRid, ToRid, RID};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SnapshotTableId {
    /// Physical database. Empty until database-aware snapshot extraction is enabled.
    pub db: String,
    /// Existing first-level namespace; extractors keep their current mapping here.
    pub schema: String,
    pub tb: String,
}

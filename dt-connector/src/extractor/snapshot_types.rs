#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SnapshotTableId {
    pub schema: String,
    pub tb: String,
}

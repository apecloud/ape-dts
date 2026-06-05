use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ZkEventType {
    Created,
    Updated,
    Deleted,
    ChildrenChanged,
}

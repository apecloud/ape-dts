use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    pin::pin,
    sync::{Arc, Mutex, MutexGuard},
};

use anyhow::{bail, Context, Result};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::{timeout, Duration};

use crate::{
    config::limiter_config::RateLimiterConfig,
    limiter::buffer_limiter::BufferLimiter,
    meta::{
        dt_data::{DtData, DtItem},
        struct_meta::statement::struct_statement::StructStatement,
    },
};

// Bound graph lock hold time even when a struct fetch returns thousands of objects.
const DT_ITEM_PUSH_BATCH_SIZE: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StructObjectType {
    Schema,
    Table,
    Collection,
    ShardKey,
    Udf,
    Udt,
    Rbac,
    Index,
    Constraint,
    Sequence,
    Custom(Arc<str>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StructDependencyKey {
    Oid {
        source_id: u32,
        object_type: StructObjectType,
        oid: u64,
    },
    Name {
        object_type: StructObjectType,
        schema: Arc<str>,
        name: Arc<str>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CdcKeyType {
    Primary,
    Unique(Arc<str>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CdcDependencyKey {
    pub schema: Arc<str>,
    pub table: Arc<str>,
    pub key_type: CdcKeyType,
    pub value_hash: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum DependencyKey {
    #[default]
    /// The node has no dependency key. It is ignored when building graph edges.
    None,
    Struct(StructDependencyKey),
    Cdc(CdcDependencyKey),
    Custom(Arc<str>),
}

#[derive(Debug, Clone, Default)]
pub struct DependencySpec {
    /// Objects that must have been provided and completed before this node can run.
    pub requires: Vec<DependencyKey>,
    /// Objects that are dependencies only when their provider was accepted by
    /// this queue. This supports filtered snapshots whose parent already exists
    /// at the target.
    pub requires_if_present: Vec<DependencyKey>,
    /// Conflict keys whose nodes must run in push order, for example a PK or UK value.
    pub ordered_by: Vec<DependencyKey>,
    /// Objects created by this node and made available after it is acknowledged.
    pub provides: Vec<DependencyKey>,
}

pub struct DependencyInput<T> {
    pub payload: T,
    pub dependencies: DependencySpec,
}

impl<T> DependencyInput<T> {
    pub fn new(payload: T, dependencies: DependencySpec) -> Self {
        Self {
            payload,
            dependencies,
        }
    }
}

struct PreparedDependencyInput<T> {
    payload: T,
    requires: Vec<DependencyKey>,
    requires_if_present: Vec<DependencyKey>,
    ordered_keys: Vec<DependencyKey>,
    provided_keys: Vec<DependencyKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeState {
    Waiting,
    Ready,
    Running,
}

struct DependencyNode<T> {
    payload: Option<T>,
    in_degree: u32,
    successors: Vec<NodeId>,
    ordered_keys: Vec<DependencyKey>,
    provided_keys: Vec<DependencyKey>,
    state: NodeState,
    _capacity_permit: OwnedSemaphorePermit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderState {
    Pending(NodeId),
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueueState {
    Open,
    Closed,
    Failed(Arc<str>),
}

struct DependencyQueueInner<T> {
    next_node_id: u64,
    nodes: HashMap<NodeId, DependencyNode<T>>,
    ready: VecDeque<NodeId>,
    tail_by_key: HashMap<DependencyKey, NodeId>,
    provider_by_key: HashMap<DependencyKey, ProviderState>,
    state: QueueState,
}

impl<T> Default for DependencyQueueInner<T> {
    fn default() -> Self {
        Self {
            next_node_id: 0,
            nodes: HashMap::new(),
            ready: VecDeque::new(),
            tail_by_key: HashMap::new(),
            provider_by_key: HashMap::new(),
            state: QueueState::Open,
        }
    }
}

pub struct ReadyNode<T> {
    pub id: NodeId,
    pub payload: T,
}

enum PopReadyState<T> {
    Ready(Vec<ReadyNode<T>>),
    Wait,
    Closed,
    Failed(Arc<str>),
}

/// A bounded dependency-aware queue.
///
/// `push` blocks while `capacity` nodes are waiting, ready, or running. `pop_ready`
/// only returns nodes whose predecessors have been acknowledged. The capacity is
/// released by `ack`, not by `pop_ready`, so in-flight work remains bounded.
pub struct DependencyQueue<T> {
    inner: Mutex<DependencyQueueInner<T>>,
    capacity: Arc<Semaphore>,
    capacity_limit: usize,
    ready_notify: Notify,
    empty_notify: Notify,
    rate_limiter: Option<BufferLimiter>,
}

impl<T> DependencyQueue<T> {
    pub fn new(capacity: usize) -> Self {
        Self::with_rate_limiter(capacity, None)
    }

    /// Queue capacity is owned by `capacity` so its permit lives until ack/fail.
    /// Accepting the rate config rather than a `BufferLimiter` prevents a
    /// capacity limiter from being accidentally installed without an ack-time
    /// release path.
    pub fn with_rate_limiter(
        capacity: usize,
        rate_limiter_config: Option<&RateLimiterConfig>,
    ) -> Self {
        assert!(capacity > 0, "dependency queue capacity must be positive");
        Self {
            inner: Mutex::new(DependencyQueueInner::default()),
            capacity: Arc::new(Semaphore::new(capacity)),
            capacity_limit: capacity,
            ready_notify: Notify::new(),
            empty_notify: Notify::new(),
            rate_limiter: BufferLimiter::from_config(rate_limiter_config, None),
        }
    }

    #[inline]
    fn lock_inner(&self) -> MutexGuard<'_, DependencyQueueInner<T>> {
        self.inner
            .lock()
            .expect("dependency queue state mutex is poisoned")
    }

    fn pop_ready_batch_state(&self, limit: usize) -> Result<PopReadyState<T>> {
        let mut inner = self.lock_inner();
        if let QueueState::Failed(error) = &inner.state {
            return Ok(PopReadyState::Failed(error.clone()));
        }
        let nodes = take_ready_batch(&mut inner, limit)?;
        if !nodes.is_empty() {
            return Ok(PopReadyState::Ready(nodes));
        }
        if inner.state == QueueState::Closed && inner.nodes.is_empty() {
            return Ok(PopReadyState::Closed);
        }
        Ok(PopReadyState::Wait)
    }

    pub async fn push(&self, input: DependencyInput<T>) -> Result<NodeId> {
        let input = prepare_input(input);
        let permit = self
            .capacity
            .clone()
            .acquire_owned()
            .await
            .context("dependency queue is closed")?;
        let (id, is_ready) = {
            let mut inner = self.lock_inner();
            ensure_open(&inner)?;
            validate_batch(&inner, std::slice::from_ref(&input))?;
            insert_prepared(&mut inner, input, permit)?
        };
        if is_ready {
            self.ready_notify.notify_one();
        }
        Ok(id)
    }

    /// Pushes an ordered batch with one graph lock acquisition.
    ///
    /// The batch must fit within the queue's configured node capacity. Items are
    /// validated as an ordered sequence, so a later item may depend on an
    /// earlier provider in the same batch.
    pub async fn push_batch(&self, inputs: Vec<DependencyInput<T>>) -> Result<Vec<NodeId>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        if inputs.len() > self.capacity_limit {
            bail!(
                "dependency batch size {} exceeds queue capacity {}",
                inputs.len(),
                self.capacity_limit
            );
        }

        let inputs: Vec<_> = inputs.into_iter().map(prepare_input).collect();
        self.push_prepared_batch(inputs).await
    }

    /// Waits for ready work. Returns `None` after `close` and all accepted nodes
    /// have been acknowledged.
    pub async fn pop_ready(&self) -> Result<Option<ReadyNode<T>>> {
        Ok(self
            .pop_ready_batch(1)
            .await?
            .and_then(|mut nodes| nodes.pop()))
    }

    /// Waits for at least one ready node, then drains up to `limit` nodes with
    /// the same graph lock acquisition.
    pub async fn pop_ready_batch(&self, limit: usize) -> Result<Option<Vec<ReadyNode<T>>>> {
        if limit == 0 {
            return Ok(Some(Vec::new()));
        }
        loop {
            // Register before checking the queue so a notification between the
            // check and await cannot be lost.
            let notified = self.ready_notify.notified();
            let mut notified = pin!(notified);
            notified.as_mut().enable();

            match self.pop_ready_batch_state(limit)? {
                PopReadyState::Ready(nodes) => return Ok(Some(nodes)),
                PopReadyState::Closed => return Ok(None),
                PopReadyState::Failed(error) => {
                    bail!("dependency queue failed: {error}")
                }
                PopReadyState::Wait => {}
            }
            notified.await;
        }
    }

    pub async fn try_pop_ready(&self) -> Result<Option<ReadyNode<T>>> {
        Ok(self.try_pop_ready_batch(1).await?.pop())
    }

    /// Takes up to `limit` currently-ready nodes under one queue lock.
    pub async fn try_pop_ready_batch(&self, limit: usize) -> Result<Vec<ReadyNode<T>>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut inner = self.lock_inner();
        if let QueueState::Failed(error) = &inner.state {
            bail!("dependency queue failed: {error}");
        }

        take_ready_batch(&mut inner, limit)
    }

    /// Marks a running node as successfully consumed and releases its successors.
    pub async fn ack(&self, id: NodeId) -> Result<()> {
        self.ack_batch([id]).await
    }

    /// Acknowledges a completed batch under one queue lock.
    pub async fn ack_batch(&self, ids: impl IntoIterator<Item = NodeId>) -> Result<()> {
        let ids: Vec<_> = ids.into_iter().collect();
        if ids.is_empty() {
            return Ok(());
        }
        if ids.iter().copied().collect::<HashSet<_>>().len() != ids.len() {
            bail!("an acknowledgement batch contains duplicate node ids");
        }

        let mut inner = self.lock_inner();
        if let QueueState::Failed(error) = &inner.state {
            bail!("dependency queue failed: {error}");
        }

        for id in &ids {
            let state = inner
                .nodes
                .get(id)
                .context("node to acknowledge is missing")?
                .state;
            if state != NodeState::Running {
                bail!("only a running node can be acknowledged: {id:?}");
            }
        }

        let mut released_ready = false;
        for id in ids {
            let node = inner
                .nodes
                .remove(&id)
                .context("node to acknowledge is missing")?;

            for key in &node.provided_keys {
                inner
                    .provider_by_key
                    .insert(key.clone(), ProviderState::Completed);
            }
            for key in &node.ordered_keys {
                if inner.tail_by_key.get(key) == Some(&id) {
                    inner.tail_by_key.remove(key);
                }
            }

            for successor_id in node.successors {
                let successor = inner
                    .nodes
                    .get_mut(&successor_id)
                    .context("successor node is missing")?;
                if successor.in_degree == 0 {
                    bail!("successor in-degree underflow: {successor_id:?}");
                }
                successor.in_degree -= 1;
                if successor.in_degree == 0 {
                    successor.state = NodeState::Ready;
                    inner.ready.push_back(successor_id);
                    released_ready = true;
                }
            }
        }

        let is_empty = inner.nodes.is_empty();
        drop(inner);
        if released_ready || is_empty {
            self.ready_notify.notify_waiters();
        }
        if is_empty {
            self.empty_notify.notify_waiters();
        }
        Ok(())
    }

    /// Returns failed work to the ready queue without copying its payload.
    pub async fn nack(&self, ready_node: ReadyNode<T>) -> Result<()> {
        let mut inner = self.lock_inner();
        match &inner.state {
            QueueState::Open | QueueState::Closed => {}
            QueueState::Failed(error) => bail!("dependency queue failed: {error}"),
        }

        let node = inner
            .nodes
            .get_mut(&ready_node.id)
            .context("node to negatively acknowledge is missing")?;
        if node.state != NodeState::Running || node.payload.is_some() {
            bail!(
                "only a running node can be negatively acknowledged: {:?}",
                ready_node.id
            );
        }
        node.payload = Some(ready_node.payload);
        node.state = NodeState::Ready;
        inner.ready.push_front(ready_node.id);
        drop(inner);
        self.ready_notify.notify_one();
        Ok(())
    }

    /// Stops accepting new nodes and lets accepted nodes drain normally.
    pub async fn close(&self) {
        let mut inner = self.lock_inner();
        if inner.state == QueueState::Open {
            inner.state = QueueState::Closed;
            self.capacity.close();
        }
        drop(inner);
        self.ready_notify.notify_waiters();
    }

    /// Terminates the queue immediately and wakes all blocked producers and consumers.
    pub async fn fail(&self, error: impl Into<Arc<str>>) {
        let mut inner = self.lock_inner();
        if matches!(inner.state, QueueState::Failed(_)) {
            return;
        }
        inner.state = QueueState::Failed(error.into());
        inner.nodes.clear();
        inner.ready.clear();
        inner.tail_by_key.clear();
        inner.provider_by_key.clear();
        self.capacity.close();
        drop(inner);
        self.ready_notify.notify_waiters();
        self.empty_notify.notify_waiters();
    }

    pub async fn wait_until_empty(&self) {
        loop {
            let notified = self.empty_notify.notified();
            let mut notified = pin!(notified);
            notified.as_mut().enable();

            if self.lock_inner().nodes.is_empty() {
                return;
            }

            notified.await;
        }
    }

    pub async fn wait_for_data(&self, max_wait: Duration) {
        let notified = self.ready_notify.notified();
        let mut notified = pin!(notified);
        notified.as_mut().enable();

        {
            let inner = self.lock_inner();
            if !inner.ready.is_empty() || inner.state != QueueState::Open {
                return;
            }
        }
        let _ = timeout(max_wait, notified).await;
    }

    pub async fn len(&self) -> usize {
        self.lock_inner().nodes.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.lock_inner().nodes.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.capacity.available_permits() == 0
    }

    pub async fn in_degree(&self, id: NodeId) -> Option<u32> {
        self.lock_inner().nodes.get(&id).map(|v| v.in_degree)
    }
}

impl DependencyQueue<DtItem> {
    /// Builds dependencies from the item type and inserts the original item
    /// without cloning its payload.
    pub async fn push_item(&self, item: DtItem) -> Result<NodeId> {
        let dependencies = build_dt_item_dependencies(&item)?;
        if let Some(limiter) = &self.rate_limiter {
            limiter.acquire(&item).await?;
        }
        self.push(DependencyInput::new(item, dependencies)).await
    }

    pub async fn push_items_batch(&self, items: Vec<DtItem>) -> Result<Vec<NodeId>> {
        let mut ids = Vec::with_capacity(items.len());
        let mut inputs = Vec::with_capacity(items.len());
        for item in items {
            let dependencies = build_dt_item_dependencies(&item)?;
            inputs.push(prepare_input(DependencyInput::new(item, dependencies)));
        }
        let inputs = order_prepared_inputs(inputs)?;
        let mut inputs = inputs.into_iter();
        let chunk_size = self.capacity_limit.min(DT_ITEM_PUSH_BATCH_SIZE);
        loop {
            let chunk: Vec<_> = inputs.by_ref().take(chunk_size).collect();
            if chunk.is_empty() {
                return Ok(ids);
            }
            if let Some(limiter) = &self.rate_limiter {
                for input in &chunk {
                    limiter.acquire(&input.payload).await?;
                }
            }
            ids.extend(self.push_prepared_batch(chunk).await?);
        }
    }
}

impl<T> DependencyQueue<T> {
    async fn push_prepared_batch(
        &self,
        inputs: Vec<PreparedDependencyInput<T>>,
    ) -> Result<Vec<NodeId>> {
        let permit_count = u32::try_from(inputs.len())
            .context("dependency batch size exceeds semaphore permit limit")?;
        let mut batch_permit = self
            .capacity
            .clone()
            .acquire_many_owned(permit_count)
            .await
            .context("dependency queue is closed")?;
        let permits: Vec<_> = (0..inputs.len())
            .map(|_| {
                batch_permit
                    .split(1)
                    .expect("batch permit count must match input count")
            })
            .collect();

        let mut ready_count = 0;
        let ids = {
            let mut inner = self.lock_inner();
            ensure_open(&inner)?;
            validate_batch(&inner, &inputs)?;
            let mut ids = Vec::with_capacity(inputs.len());
            for (input, permit) in inputs.into_iter().zip(permits) {
                let (id, is_ready) = insert_prepared(&mut inner, input, permit)?;
                ready_count += usize::from(is_ready);
                ids.push(id);
            }
            ids
        };
        if ready_count > 0 {
            self.ready_notify.notify_waiters();
        }
        Ok(ids)
    }
}

fn build_dt_item_dependencies(item: &DtItem) -> Result<DependencySpec> {
    let DtData::Struct { struct_data } = &item.dt_data else {
        bail!("dependency queue currently only supports struct items");
    };

    let object_key = item.dt_data.object_key()?;
    let parent_keys = item.dt_data.parent_object_keys()?;
    let mut spec = DependencySpec::default();
    if matches!(struct_data.statement, StructStatement::PgCreateRbac(_)) {
        if object_key != DependencyKey::None {
            spec.ordered_by.push(object_key);
        }
    } else if object_key != DependencyKey::None {
        spec.provides.push(object_key);
    }
    spec.requires_if_present.extend(parent_keys);
    Ok(spec)
}

fn prepare_input<T>(input: DependencyInput<T>) -> PreparedDependencyInput<T> {
    PreparedDependencyInput {
        payload: input.payload,
        requires: normalize_keys(input.dependencies.requires),
        requires_if_present: normalize_keys(input.dependencies.requires_if_present),
        ordered_keys: normalize_keys(input.dependencies.ordered_by),
        provided_keys: normalize_keys(input.dependencies.provides),
    }
}

/// Orders one fetched Struct batch so providers precede their consumers while
/// preserving extractor order for otherwise-independent nodes.
fn order_prepared_inputs<T>(
    inputs: Vec<PreparedDependencyInput<T>>,
) -> Result<Vec<PreparedDependencyInput<T>>> {
    let mut provider_indexes = HashMap::new();
    for (index, input) in inputs.iter().enumerate() {
        for key in &input.provided_keys {
            if provider_indexes.insert(key.clone(), index).is_some() {
                bail!("dependency key is provided more than once in a struct batch: {key:?}");
            }
        }
    }

    let mut in_degrees = vec![0_u32; inputs.len()];
    let mut successors = vec![Vec::new(); inputs.len()];
    let mut last_ordered_by = HashMap::new();
    for (index, input) in inputs.iter().enumerate() {
        let mut predecessors = HashSet::new();
        for key in input.requires.iter().chain(&input.requires_if_present) {
            if let Some(predecessor) = provider_indexes.get(key).copied() {
                if predecessor != index {
                    predecessors.insert(predecessor);
                }
            }
        }
        for key in &input.ordered_keys {
            if let Some(predecessor) = last_ordered_by.insert(key.clone(), index) {
                predecessors.insert(predecessor);
            }
        }
        in_degrees[index] = predecessors.len() as u32;
        for predecessor in predecessors {
            successors[predecessor].push(index);
        }
    }

    let mut ready = BinaryHeap::new();
    for (index, in_degree) in in_degrees.iter().enumerate() {
        if *in_degree == 0 {
            ready.push(Reverse(index));
        }
    }

    let input_count = inputs.len();
    let mut inputs: Vec<_> = inputs.into_iter().map(Some).collect();
    let mut ordered = Vec::with_capacity(input_count);
    while let Some(Reverse(index)) = ready.pop() {
        ordered.push(
            inputs[index]
                .take()
                .context("struct batch node is missing")?,
        );
        for successor in &successors[index] {
            in_degrees[*successor] -= 1;
            if in_degrees[*successor] == 0 {
                ready.push(Reverse(*successor));
            }
        }
    }
    if ordered.len() != input_count {
        bail!("struct batch contains a dependency cycle");
    }
    Ok(ordered)
}

fn ensure_open<T>(inner: &DependencyQueueInner<T>) -> Result<()> {
    match &inner.state {
        QueueState::Open => Ok(()),
        QueueState::Closed => bail!("dependency queue is closed"),
        QueueState::Failed(error) => bail!("dependency queue failed: {error}"),
    }
}

fn validate_batch<T>(
    inner: &DependencyQueueInner<T>,
    inputs: &[PreparedDependencyInput<T>],
) -> Result<()> {
    let mut batch_providers = HashSet::new();
    for input in inputs {
        for key in &input.requires {
            if !inner.provider_by_key.contains_key(key) && !batch_providers.contains(key) {
                bail!("required dependency has not been pushed: {key:?}");
            }
        }
        for key in &input.provided_keys {
            if inner.provider_by_key.contains_key(key) || !batch_providers.insert(key.clone()) {
                bail!("dependency key is provided more than once: {key:?}");
            }
        }
    }
    Ok(())
}

fn insert_prepared<T>(
    inner: &mut DependencyQueueInner<T>,
    input: PreparedDependencyInput<T>,
    permit: OwnedSemaphorePermit,
) -> Result<(NodeId, bool)> {
    let PreparedDependencyInput {
        payload,
        requires,
        requires_if_present,
        ordered_keys,
        provided_keys,
    } = input;

    let mut predecessors = HashSet::new();
    for key in requires {
        match inner.provider_by_key.get(&key) {
            Some(ProviderState::Pending(node_id)) => {
                predecessors.insert(*node_id);
            }
            Some(ProviderState::Completed) => {}
            None => bail!("required dependency has not been pushed: {key:?}"),
        }
    }
    for key in requires_if_present {
        match inner.provider_by_key.get(&key) {
            Some(ProviderState::Pending(node_id)) => {
                predecessors.insert(*node_id);
            }
            Some(ProviderState::Completed) | None => {}
        }
    }
    for key in &ordered_keys {
        if let Some(node_id) = inner.tail_by_key.get(key) {
            predecessors.insert(*node_id);
        }
    }

    let id = NodeId(inner.next_node_id);
    inner.next_node_id += 1;
    let in_degree = predecessors.len() as u32;
    let is_ready = in_degree == 0;
    let state = if is_ready {
        NodeState::Ready
    } else {
        NodeState::Waiting
    };

    inner.nodes.insert(
        id,
        DependencyNode {
            payload: Some(payload),
            in_degree,
            successors: Vec::new(),
            ordered_keys: ordered_keys.clone(),
            provided_keys: provided_keys.clone(),
            state,
            _capacity_permit: permit,
        },
    );
    for predecessor in predecessors {
        inner
            .nodes
            .get_mut(&predecessor)
            .context("predecessor node is missing")?
            .successors
            .push(id);
    }
    for key in ordered_keys {
        inner.tail_by_key.insert(key, id);
    }
    for key in provided_keys {
        inner
            .provider_by_key
            .insert(key, ProviderState::Pending(id));
    }
    if is_ready {
        inner.ready.push_back(id);
    }
    Ok((id, is_ready))
}

fn normalize_keys(values: Vec<DependencyKey>) -> Vec<DependencyKey> {
    let mut seen = HashSet::with_capacity(values.len());
    values
        .into_iter()
        .filter(|value| *value != DependencyKey::None)
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

fn take_ready_batch<T>(
    inner: &mut DependencyQueueInner<T>,
    limit: usize,
) -> Result<Vec<ReadyNode<T>>> {
    let count = limit.min(inner.ready.len());
    let mut ready_nodes = Vec::with_capacity(count);
    for _ in 0..count {
        let id = inner
            .ready
            .pop_front()
            .context("ready queue length changed while draining")?;
        let node = inner.nodes.get_mut(&id).context("ready node is missing")?;
        if node.state != NodeState::Ready || node.in_degree != 0 {
            bail!("ready queue contains a node that is not ready: {id:?}");
        }
        node.state = NodeState::Running;
        let payload = node
            .payload
            .take()
            .context("ready node payload is missing")?;
        ready_nodes.push(ReadyNode { id, payload });
    }
    Ok(ready_nodes)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::time::timeout;

    use super::*;
    use crate::meta::{
        position::Position,
        struct_meta::{
            statement::{
                mysql_create_database_statement::MysqlCreateDatabaseStatement,
                mysql_create_table_statement::MysqlCreateTableStatement,
                struct_statement::StructStatement,
            },
            struct_data::StructData,
            structure::{
                constraint::{Constraint, ConstraintType},
                database::Database,
                table::Table,
            },
        },
    };

    fn custom_key(value: &str) -> DependencyKey {
        DependencyKey::Custom(Arc::from(value))
    }

    fn pk(value_hash: u128) -> DependencyKey {
        DependencyKey::Cdc(CdcDependencyKey {
            schema: Arc::from("public"),
            table: Arc::from("users"),
            key_type: CdcKeyType::Primary,
            value_hash,
        })
    }

    fn uk(name: &str, value_hash: u128) -> DependencyKey {
        DependencyKey::Cdc(CdcDependencyKey {
            schema: Arc::from("public"),
            table: Arc::from("users"),
            key_type: CdcKeyType::Unique(Arc::from(name)),
            value_hash,
        })
    }

    fn struct_item(statement: StructStatement) -> DtItem {
        DtItem {
            dt_data: DtData::Struct {
                struct_data: StructData {
                    schema: String::new(),
                    statement,
                },
            },
            position: Position::None,
            data_origin_node: String::new(),
        }
    }

    fn mysql_database(name: &str) -> DtItem {
        struct_item(StructStatement::MysqlCreateDatabase(
            MysqlCreateDatabaseStatement {
                database: Database {
                    name: name.to_string(),
                    ..Default::default()
                },
            },
        ))
    }

    fn mysql_table(database: &str, table: &str) -> DtItem {
        mysql_table_with_parents(database, table, &[])
    }

    fn mysql_table_with_parents(database: &str, table: &str, parents: &[&str]) -> DtItem {
        struct_item(StructStatement::MysqlCreateTable(
            MysqlCreateTableStatement {
                table: Table {
                    database_name: database.to_string(),
                    table_name: table.to_string(),
                    ..Default::default()
                },
                constraints: parents
                    .iter()
                    .map(|parent| Constraint {
                        database_name: database.to_string(),
                        schema_name: String::new(),
                        table_name: table.to_string(),
                        constraint_name: format!("fk_{table}_{parent}"),
                        constraint_type: ConstraintType::Foreign,
                        definition: String::new(),
                        referenced_database_name: database.to_string(),
                        referenced_schema_name: String::new(),
                        referenced_table_name: (*parent).to_string(),
                    })
                    .collect(),
                indexes: Vec::new(),
            },
        ))
    }

    #[tokio::test]
    async fn dt_item_table_waits_for_accepted_database() {
        let queue = DependencyQueue::new(2);
        let ids = queue
            .push_items_batch(vec![mysql_database("db"), mysql_table("db", "tb")])
            .await
            .unwrap();
        let [database_id, table_id] = ids.as_slice() else {
            panic!("expected database and table node ids");
        };

        assert_eq!(queue.in_degree(*table_id).await, Some(1));
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, *database_id);
        assert!(queue.try_pop_ready().await.unwrap().is_none());

        queue.ack(*database_id).await.unwrap();
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, *table_id);
    }

    #[tokio::test]
    async fn dt_item_table_does_not_wait_for_filtered_database() {
        let queue = DependencyQueue::new(1);
        let table_id = queue
            .push_item(mysql_table("external", "tb"))
            .await
            .unwrap();

        assert_eq!(queue.in_degree(table_id).await, Some(0));
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, table_id);
    }

    #[tokio::test]
    async fn struct_batch_orders_and_waits_for_multiple_fk_parents() {
        let queue = DependencyQueue::new(4);
        let ids = queue
            .push_items_batch(vec![
                mysql_database("db"),
                mysql_table_with_parents("db", "child", &["parent_a", "parent_b"]),
                mysql_table("db", "parent_b"),
                mysql_table("db", "parent_a"),
            ])
            .await
            .unwrap();
        let [database_id, parent_b_id, parent_a_id, child_id] = ids.as_slice() else {
            panic!("expected four node ids");
        };

        assert_eq!(queue.in_degree(*child_id).await, Some(3));
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, *database_id);
        queue.ack(*database_id).await.unwrap();

        let parents = queue.pop_ready_batch(2).await.unwrap().unwrap();
        assert_eq!(
            parents.iter().map(|node| node.id).collect::<Vec<_>>(),
            vec![*parent_b_id, *parent_a_id]
        );
        queue.ack(*parent_b_id).await.unwrap();
        assert_eq!(queue.in_degree(*child_id).await, Some(1));
        assert!(queue.try_pop_ready().await.unwrap().is_none());

        queue.ack(*parent_a_id).await.unwrap();
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, *child_id);
    }

    #[tokio::test]
    async fn dt_item_rejects_unsupported_item_type() {
        let queue = DependencyQueue::new(1);
        let result = queue
            .push_item(DtItem {
                dt_data: DtData::Heartbeat {},
                position: Position::None,
                data_origin_node: String::new(),
            })
            .await;

        assert!(result.is_err());
        assert_eq!(queue.len().await, 0);
    }

    #[tokio::test]
    async fn explicit_dependency_is_released_only_after_predecessor_ack() {
        let queue = DependencyQueue::new(4);
        let table = custom_key("table.public.users");

        let table_id = queue
            .push(DependencyInput::new(
                "create table",
                DependencySpec {
                    provides: vec![table.clone()],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        let index_id = queue
            .push(DependencyInput::new(
                "create index",
                DependencySpec {
                    requires: vec![table],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

        assert_eq!(queue.in_degree(index_id).await, Some(1));
        let ready = queue.pop_ready().await.unwrap().unwrap();
        assert_eq!(ready.id, table_id);
        assert_eq!(ready.payload, "create table");
        assert!(queue.try_pop_ready().await.unwrap().is_none());

        queue.ack(table_id).await.unwrap();
        assert_eq!(queue.in_degree(index_id).await, Some(0));
        let ready = queue.pop_ready().await.unwrap().unwrap();
        assert_eq!(ready.id, index_id);
        assert_eq!(ready.payload, "create index");
    }

    #[tokio::test]
    async fn pk_and_uk_collisions_create_dependencies() {
        let queue = DependencyQueue::new(8);
        let first_id = queue
            .push(DependencyInput::new(
                "first",
                DependencySpec {
                    ordered_by: vec![pk(1), uk("email", 11)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        let same_uk_id = queue
            .push(DependencyInput::new(
                "same uk",
                DependencySpec {
                    ordered_by: vec![pk(2), uk("email", 11)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        let same_pk_id = queue
            .push(DependencyInput::new(
                "same pk",
                DependencySpec {
                    ordered_by: vec![pk(1), uk("email", 12)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

        assert_eq!(queue.in_degree(same_uk_id).await, Some(1));
        assert_eq!(queue.in_degree(same_pk_id).await, Some(1));
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, first_id);
        assert!(queue.try_pop_ready().await.unwrap().is_none());

        queue.ack(first_id).await.unwrap();
        let mut ready_ids = vec![
            queue.pop_ready().await.unwrap().unwrap().id,
            queue.pop_ready().await.unwrap().unwrap().id,
        ];
        ready_ids.sort_by_key(|id| id.0);
        assert_eq!(ready_ids, vec![same_uk_id, same_pk_id]);
    }

    #[tokio::test]
    async fn dynamically_pushed_node_depends_on_running_tail() {
        let queue = DependencyQueue::new(4);
        let first_id = queue
            .push(DependencyInput::new(
                "first",
                DependencySpec {
                    ordered_by: vec![pk(1)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, first_id);

        let second_id = queue
            .push(DependencyInput::new(
                "second",
                DependencySpec {
                    ordered_by: vec![pk(1)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        let independent_id = queue
            .push(DependencyInput::new(
                "independent",
                DependencySpec {
                    ordered_by: vec![pk(2)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

        assert_eq!(queue.in_degree(second_id).await, Some(1));
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, independent_id);
        assert!(queue.try_pop_ready().await.unwrap().is_none());

        queue.ack(first_id).await.unwrap();
        assert_eq!(queue.in_degree(second_id).await, Some(0));
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, second_id);
    }

    #[tokio::test]
    async fn node_waits_for_all_colliding_predecessors() {
        let queue = DependencyQueue::new(4);
        let pk_predecessor = queue
            .push(DependencyInput::new(
                "pk predecessor",
                DependencySpec {
                    ordered_by: vec![pk(1)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        let uk_predecessor = queue
            .push(DependencyInput::new(
                "uk predecessor",
                DependencySpec {
                    ordered_by: vec![uk("email", 11)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        let dependent = queue
            .push(DependencyInput::new(
                "dependent",
                DependencySpec {
                    ordered_by: vec![pk(1), uk("email", 11)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

        assert_eq!(queue.in_degree(dependent).await, Some(2));
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, pk_predecessor);
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, uk_predecessor);

        queue.ack(pk_predecessor).await.unwrap();
        assert_eq!(queue.in_degree(dependent).await, Some(1));
        assert!(queue.try_pop_ready().await.unwrap().is_none());

        queue.ack(uk_predecessor).await.unwrap();
        assert_eq!(queue.in_degree(dependent).await, Some(0));
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, dependent);
    }

    #[tokio::test]
    async fn duplicate_collision_keys_count_as_one_predecessor() {
        let queue = DependencyQueue::new(4);
        let first_id = queue
            .push(DependencyInput::new(
                "first",
                DependencySpec {
                    ordered_by: vec![pk(1)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        let second_id = queue
            .push(DependencyInput::new(
                "second",
                DependencySpec {
                    ordered_by: vec![pk(1), pk(1)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

        assert_eq!(queue.in_degree(second_id).await, Some(1));
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, first_id);
        queue.ack(first_id).await.unwrap();
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, second_id);
    }

    #[tokio::test]
    async fn batch_pop_and_ack_release_shared_successor() {
        let queue = DependencyQueue::new(3);
        let first = queue
            .push(DependencyInput::new(
                "first",
                DependencySpec {
                    ordered_by: vec![pk(1)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        let second = queue
            .push(DependencyInput::new(
                "second",
                DependencySpec {
                    ordered_by: vec![uk("email", 2)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        let successor = queue
            .push(DependencyInput::new(
                "successor",
                DependencySpec {
                    ordered_by: vec![pk(1), uk("email", 2)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

        let ready = queue.try_pop_ready_batch(8).await.unwrap();
        assert_eq!(
            ready.iter().map(|node| node.id).collect::<Vec<_>>(),
            vec![first, second]
        );
        queue
            .ack_batch(ready.into_iter().map(|node| node.id))
            .await
            .unwrap();
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, successor);
    }

    #[tokio::test]
    async fn push_blocks_until_running_node_is_acknowledged() {
        let queue = Arc::new(DependencyQueue::new(1));
        let first_id = queue
            .push(DependencyInput::new("first", DependencySpec::default()))
            .await
            .unwrap();

        let push_queue = queue.clone();
        let second_push = tokio::spawn(async move {
            push_queue
                .push(DependencyInput::new("second", DependencySpec::default()))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!second_push.is_finished());

        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, first_id);
        assert!(!second_push.is_finished());
        queue.ack(first_id).await.unwrap();

        let second_id = timeout(Duration::from_secs(1), second_push)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, second_id);
    }

    #[tokio::test]
    async fn payload_is_moved_without_cloning() {
        let queue = DependencyQueue::new(1);
        let payload = Arc::new(String::from("payload"));
        let pointer = Arc::as_ptr(&payload);

        queue
            .push(DependencyInput::new(payload, DependencySpec::default()))
            .await
            .unwrap();
        let ready = queue.pop_ready().await.unwrap().unwrap();

        assert_eq!(Arc::as_ptr(&ready.payload), pointer);
        assert_eq!(Arc::strong_count(&ready.payload), 1);
    }

    #[tokio::test]
    async fn nack_moves_payload_back_and_requeues_the_node() {
        let queue = DependencyQueue::new(1);
        let id = queue
            .push(DependencyInput::new(
                String::from("retry me"),
                DependencySpec::default(),
            ))
            .await
            .unwrap();
        let ready = queue.pop_ready().await.unwrap().unwrap();
        let pointer = ready.payload.as_ptr();

        queue.nack(ready).await.unwrap();
        let retried = queue.pop_ready().await.unwrap().unwrap();
        assert_eq!(retried.id, id);
        assert_eq!(retried.payload.as_ptr(), pointer);
        assert_eq!(retried.payload, "retry me");
        queue.ack(id).await.unwrap();
    }

    #[tokio::test]
    async fn close_rejects_push_and_returns_none_after_drain() {
        let queue = DependencyQueue::new(2);
        let id = queue
            .push(DependencyInput::new("accepted", DependencySpec::default()))
            .await
            .unwrap();

        queue.close().await;
        assert!(queue
            .push(DependencyInput::new("rejected", DependencySpec::default()))
            .await
            .is_err());

        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, id);
        queue.ack(id).await.unwrap();
        assert!(queue.pop_ready().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn close_wakes_blocked_producer_and_consumer() {
        let queue = Arc::new(DependencyQueue::new(1));
        let first_id = queue
            .push(DependencyInput::new("first", DependencySpec::default()))
            .await
            .unwrap();
        let ready = queue.pop_ready().await.unwrap().unwrap();
        assert_eq!(ready.id, first_id);

        let push_queue = queue.clone();
        let blocked_push = tokio::spawn(async move {
            push_queue
                .push(DependencyInput::new("second", DependencySpec::default()))
                .await
        });
        let pop_queue = queue.clone();
        let blocked_pop = tokio::spawn(async move { pop_queue.pop_ready().await });
        tokio::task::yield_now().await;

        queue.close().await;
        assert!(timeout(Duration::from_secs(1), blocked_push)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert!(!blocked_pop.is_finished());

        queue.ack(first_id).await.unwrap();
        assert!(timeout(Duration::from_secs(1), blocked_pop)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn fail_wakes_waiters_and_releases_capacity() {
        let queue = Arc::new(DependencyQueue::new(1));
        let id = queue
            .push(DependencyInput::new("first", DependencySpec::default()))
            .await
            .unwrap();
        assert_eq!(queue.pop_ready().await.unwrap().unwrap().id, id);

        let pop_queue = queue.clone();
        let blocked_pop = tokio::spawn(async move { pop_queue.pop_ready().await });
        let push_queue = queue.clone();
        let blocked_push = tokio::spawn(async move {
            push_queue
                .push(DependencyInput::new("second", DependencySpec::default()))
                .await
        });
        tokio::task::yield_now().await;

        queue.fail("sinker failed").await;
        assert!(timeout(Duration::from_secs(1), blocked_pop)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert!(timeout(Duration::from_secs(1), blocked_push)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert_eq!(queue.len().await, 0);
    }

    #[tokio::test]
    async fn multiple_waiting_consumers_are_woken_for_ready_nodes() {
        let queue = Arc::new(DependencyQueue::new(2));
        let first_queue = queue.clone();
        let first_pop = tokio::spawn(async move { first_queue.pop_ready().await });
        let second_queue = queue.clone();
        let second_pop = tokio::spawn(async move { second_queue.pop_ready().await });
        tokio::task::yield_now().await;

        let first_id = queue
            .push(DependencyInput::new("first", DependencySpec::default()))
            .await
            .unwrap();
        let second_id = queue
            .push(DependencyInput::new("second", DependencySpec::default()))
            .await
            .unwrap();

        let mut ids = vec![
            timeout(Duration::from_secs(1), first_pop)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .unwrap()
                .id,
            timeout(Duration::from_secs(1), second_pop)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .unwrap()
                .id,
        ];
        ids.sort_by_key(|id| id.0);
        assert_eq!(ids, vec![first_id, second_id]);
    }

    #[tokio::test]
    async fn missing_explicit_dependency_is_rejected() {
        let queue = DependencyQueue::new(1);
        let result = queue
            .push(DependencyInput::new(
                "index",
                DependencySpec {
                    requires: vec![custom_key("missing table")],
                    ..Default::default()
                },
            ))
            .await;

        assert!(result.is_err());
        assert_eq!(queue.len().await, 0);
    }

    #[tokio::test]
    async fn none_dependency_key_means_no_dependency() {
        let queue = DependencyQueue::new(1);
        let id = queue
            .push(DependencyInput::new(
                "ready",
                DependencySpec {
                    requires: vec![DependencyKey::None],
                    requires_if_present: vec![DependencyKey::None],
                    ordered_by: vec![DependencyKey::None],
                    provides: vec![DependencyKey::None],
                },
            ))
            .await
            .unwrap();

        assert_eq!(queue.in_degree(id).await, Some(0));
        let ready = queue.pop_ready().await.unwrap().unwrap();
        assert_eq!(ready.id, id);
        assert_eq!(ready.payload, "ready");
    }

    #[tokio::test]
    async fn push_batch_preserves_ordered_dependencies() {
        let queue = DependencyQueue::new(4);
        let object = custom_key("table.public.users");
        let ids = queue
            .push_batch(vec![
                DependencyInput::new(
                    "create table",
                    DependencySpec {
                        provides: vec![object.clone()],
                        ..Default::default()
                    },
                ),
                DependencyInput::new(
                    "create index",
                    DependencySpec {
                        requires: vec![object],
                        ..Default::default()
                    },
                ),
            ])
            .await
            .unwrap();

        let first = queue.pop_ready_batch(8).await.unwrap().unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].id, ids[0]);
        queue.ack_batch([ids[0]]).await.unwrap();

        let second = queue.pop_ready_batch(8).await.unwrap().unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].id, ids[1]);
    }

    #[tokio::test]
    async fn invalid_batch_is_not_partially_inserted() {
        let queue = DependencyQueue::new(4);
        let missing = custom_key("missing");
        let result = queue
            .push_batch(vec![
                DependencyInput::new("ready", DependencySpec::default()),
                DependencyInput::new(
                    "invalid",
                    DependencySpec {
                        requires: vec![missing],
                        ..Default::default()
                    },
                ),
            ])
            .await;

        assert!(result.is_err());
        assert_eq!(queue.len().await, 0);
    }

    #[tokio::test]
    async fn concurrent_waiting_batches_make_progress_without_partial_insertion() {
        let queue = Arc::new(DependencyQueue::new(3));
        queue
            .push(DependencyInput::new("running", DependencySpec::default()))
            .await
            .unwrap();
        let running = queue.pop_ready().await.unwrap().unwrap();
        assert_eq!(queue.capacity.available_permits(), 2);

        let first_queue = queue.clone();
        let (first_started_tx, first_started_rx) = tokio::sync::oneshot::channel();
        let first_push = tokio::spawn(async move {
            first_started_tx.send(()).unwrap();
            first_queue
                .push_batch(vec![
                    DependencyInput::new("first-1", DependencySpec::default()),
                    DependencyInput::new("first-2", DependencySpec::default()),
                    DependencyInput::new("first-3", DependencySpec::default()),
                ])
                .await
        });
        first_started_rx.await.unwrap();
        tokio::task::yield_now().await;

        let second_queue = queue.clone();
        let (second_started_tx, second_started_rx) = tokio::sync::oneshot::channel();
        let second_push = tokio::spawn(async move {
            second_started_tx.send(()).unwrap();
            second_queue
                .push_batch(vec![
                    DependencyInput::new("second-1", DependencySpec::default()),
                    DependencyInput::new("second-2", DependencySpec::default()),
                    DependencyInput::new("second-3", DependencySpec::default()),
                ])
                .await
        });
        second_started_rx.await.unwrap();
        tokio::task::yield_now().await;

        assert!(!first_push.is_finished());
        assert!(!second_push.is_finished());
        assert_eq!(queue.len().await, 1);

        queue.ack(running.id).await.unwrap();
        let first_ids = timeout(Duration::from_secs(1), first_push)
            .await
            .expect("first batch should proceed after capacity is available")
            .unwrap()
            .unwrap();
        assert_eq!(first_ids.len(), 3);
        assert!(!second_push.is_finished());

        let ready = queue.pop_ready_batch(3).await.unwrap().unwrap();
        queue
            .ack_batch(ready.iter().map(|node| node.id))
            .await
            .unwrap();

        let second_ids = timeout(Duration::from_secs(1), second_push)
            .await
            .expect("second batch should proceed after the first batch is acknowledged")
            .unwrap()
            .unwrap();
        assert_eq!(second_ids.len(), 3);
        let ready = queue.pop_ready_batch(3).await.unwrap().unwrap();
        queue
            .ack_batch(ready.iter().map(|node| node.id))
            .await
            .unwrap();
        assert!(queue.is_empty().await);
    }

    #[tokio::test]
    async fn oversized_batch_is_rejected_without_waiting() {
        let queue = DependencyQueue::new(1);
        let result = timeout(
            Duration::from_millis(100),
            queue.push_batch(vec![
                DependencyInput::new("first", DependencySpec::default()),
                DependencyInput::new("second", DependencySpec::default()),
            ]),
        )
        .await
        .expect("oversized batch should not block");

        assert!(result.is_err());
        assert_eq!(queue.len().await, 0);
    }
}

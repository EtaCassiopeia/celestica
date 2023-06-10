use std::borrow::Cow;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use celestica_crdt::HLCTimestamp;
use celestica_node::{Clock, MembershipChange, NodeId, RpcNetwork};
use celestica_rpc::Status;
use crossbeam_channel::{Receiver, Sender};
use crossbeam_utils::atomic::AtomicCell;
use puppet::ActorMailbox;
use tokio::sync::Semaphore;
use tokio::time::{interval, Instant, MissedTickBehavior};

use crate::core::DocumentMetadata;
use crate::keyspace::{
    Del,
    Diff,
    KeyspaceActor,
    KeyspaceGroup,
    KeyspaceTimestamps,
    MultiDel,
    MultiSet,
    READ_REPAIR_SOURCE_ID,
};
use crate::replication::MAX_CONCURRENT_REQUESTS;
use crate::rpc::ReplicationClient;
use crate::storage::ProgressWatcher;
use crate::{DocVec, ProgressTracker, PutContext, Storage};

const INITIAL_KEYSPACE_WAIT: Duration = if cfg!(any(test, feature = "test-utils")) {
    Duration::from_millis(500)
} else {
    Duration::from_secs(30)
};
const KEYSPACE_SYNC_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_NUMBER_OF_DOCS_PER_FETCH: usize = 50_000;

/// The required context for the actor to run.
pub struct ReplicationCycleContext<S>
where
    S: Storage,
{
    /// The time interval which should elapse between each tick.
    pub(crate) repair_interval: Duration,
    /// The ID of the local node.
    /// The cluster keyspace group.
    pub(crate) group: KeyspaceGroup<S>,
    /// The cluster RPC network.
    pub(crate) network: RpcNetwork,
}

impl<S> ReplicationCycleContext<S>
where
    S: Storage,
{
    /// Gets the cluster clock.
    pub fn clock(&self) -> &Clock {
        self.group.clock()
    }
}

pub struct NodeChangeInfo {
    changes: Vec<KeyspaceDiff>,
}

#[derive(Clone)]
/// A handle for communicating with the replication cycle actor.
///
/// This handle is cheap to clone.
pub(crate) struct ReplicationHandle {
    tx: Sender<Op>,
    kill_switch: Arc<AtomicBool>,
}

impl ReplicationHandle {
    /// Marks that the cluster has had a membership change.
    pub(crate) fn membership_change(&self, changes: MembershipChange) {
        let _ = self.tx.send(Op::MembershipChange(changes));
    }

    /// Kills the replication service.
    pub(crate) fn kill(&self) {
        self.kill_switch.store(true, Ordering::Relaxed);
    }
}

/// A enqueued event/operation for the cycle to handle next tick.
enum Op {
    MembershipChange(MembershipChange),
}

/// Starts the replication cycle task.
///
/// This task is an intermittent task which checks to make sure current node is up to date
/// with what other nodes have seen.
///
/// In theory the difference should be minimal as the task distributor will create a higher
/// consistency unless a node goes down.
pub(crate) async fn start_replication_cycle<S>(
    ctx: ReplicationCycleContext<S>,
) -> ReplicationHandle
where
    S: Storage,
{
    let kill_switch = Arc::new(AtomicBool::new(false));
    let (tx, rx) = crossbeam_channel::unbounded();

    tokio::spawn(replication_cycle(ctx, rx, kill_switch.clone()));

    ReplicationHandle { tx, kill_switch }
}

async fn replication_cycle<S>(
    ctx: ReplicationCycleContext<S>,
    rx: Receiver<Op>,
    kill_switch: Arc<AtomicBool>,
) where
    S: Storage,
{
    let mut live_members = BTreeMap::new();
    let mut keyspace_tracker = KeyspaceTracker::default();

    let mut interval = interval(ctx.repair_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    tokio::time::sleep(INITIAL_KEYSPACE_WAIT).await;

    loop {
        interval.tick().await;

        if kill_switch.load(Ordering::Relaxed) {
            break;
        }

        while let Ok(op) = rx.try_recv() {
            match op {
                Op::MembershipChange(changes) => {
                    for member in changes.left {
                        live_members.remove(&member.node_id);
                        keyspace_tracker.remove_node(member.node_id);
                    }

                    for member in changes.joined {
                        live_members.insert(member.node_id, member.public_addr);
                    }
                },
            }
        }

        repair_members(&ctx, &live_members, &mut keyspace_tracker).await;
    }
}

#[derive(Default, Debug)]
struct KeyspaceTracker {
    inner: BTreeMap<NodeId, KeyspaceTimestamps>,
}

impl KeyspaceTracker {
    fn remove_node(&mut self, node_id: NodeId) {
        self.inner.remove(&node_id);
    }

    fn get_diff(
        &mut self,
        node_id: NodeId,
        other: &BTreeMap<String, HLCTimestamp>,
    ) -> impl Iterator<Item = Cow<'static, str>> {
        self.inner.entry(node_id).or_default().diff(other)
    }

    fn set_keyspace(&mut self, node_id: NodeId, keyspace: String, ts: HLCTimestamp) {
        self.inner
            .entry(node_id)
            .or_default()
            .insert(Cow::Owned(keyspace), Arc::new(AtomicCell::new(ts)));
    }
}

/// Polls all `live_members` and works out what entries are missing from the local node.
///
/// This will return a optimised plan of what nodes should have what documents retrieved.
async fn repair_members<S>(
    ctx: &ReplicationCycleContext<S>,
    live_members: &BTreeMap<NodeId, SocketAddr>,
    keyspace_tracker: &mut KeyspaceTracker,
) where
    S: Storage,
{
    for (node_id, addr) in live_members {
        let res = check_node_changes(ctx, *node_id, *addr, keyspace_tracker).await;

        let info = match res {
            Err(e) => {
                error!(
                    error = ?e,
                    target_node_id = %node_id,
                    target_node_addr = %addr,
                    "Failed to poll node changes due to error.",
                );
                continue;
            },
            Ok(info) => info,
        };

        for change in info.changes {
            let res = begin_keyspace_sync(
                ctx,
                change.keyspace.clone(),
                *node_id,
                *addr,
                change.removed,
                change.modified,
            )
            .await;

            if let Err(e) = res {
                error!(
                    error = ?e,
                    keyspace = %change.keyspace,
                    target_node_id = %node_id,
                    target_node_addr = %addr,
                    "Failed to sync with node."
                );
            } else {
                keyspace_tracker.set_keyspace(
                    *node_id,
                    change.keyspace,
                    change.last_updated,
                );
            }
        }
    }
}

/// Polls the remote node's keyspace timestamps.
///
/// If any timestamps are different to when the node was last polled, a task is created
/// for each keyspace which has changed.
///
/// If a task already exists for a given keyspace, it is checked to see if the task is complete
/// or not, if the task is complete then a new task is created, otherwise, if the task is *not*
/// complete but has taken longer than the allowed timeout period, the existing task is cancelled
/// and restarted.
async fn check_node_changes<S>(
    ctx: &ReplicationCycleContext<S>,
    target_node_id: NodeId,
    target_node_addr: SocketAddr,
    keyspace_tracker: &mut KeyspaceTracker,
) -> Result<NodeChangeInfo, anyhow::Error>
where
    S: Storage,
{
    info!(
        target_node_id = %target_node_id,
        target_node_addr = %target_node_addr,
        "Getting keyspace changes on remote node.",
    );

    let channel = ctx.network.get_or_connect(target_node_addr);
    let mut client = ReplicationClient::<S>::new(ctx.clock().clone(), channel.clone());
    let keyspace_timestamps = client.poll_keyspace().await?;

    let diff = keyspace_tracker
        .get_diff(target_node_id, &keyspace_timestamps)
        .map(|ks| ks.to_string())
        .collect::<Vec<_>>();

    let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
    let mut tasks = Vec::new();
    for keyspace in diff {
        let permits = permits.clone();
        let group = ctx.group.clone();
        let client = ReplicationClient::new(ctx.clock().clone(), channel.clone());

        let task = tokio::spawn(async move {
            let _permit = permits.acquire();
            get_keyspace_diff(
                keyspace,
                target_node_id.to_string(),
                target_node_addr,
                group,
                client,
            )
            .await
        });
        tasks.push(task);
    }

    let mut changes = Vec::new();
    for task in tasks {
        let diff = task.await?;
        match diff {
            Err(e) => {
                error!(
                    error = ?e.cause,
                    keyspace = %e.keyspace,
                    target_node_id = %e.node_id,
                    target_rpc_addr = %e.node_addr,
                    "Failed to get keyspace diff.",
                );
            },
            Ok(diff) => {
                changes.push(diff);
            },
        }
    }

    Ok(NodeChangeInfo { changes })
}

pub struct KeyspaceDiff {
    keyspace: String,
    modified: DocVec<DocumentMetadata>,
    removed: DocVec<DocumentMetadata>,
    last_updated: HLCTimestamp,
}

#[derive(Debug, thiserror::Error)]
#[error("RPC Error: {cause:?}")]
pub struct GetDiffError {
    cause: Status,
    keyspace: String,
    node_id: String,
    node_addr: SocketAddr,
}

#[instrument(
name = "keyspace-diff",
skip_all,
fields(
keyspace = %keyspace_name,
target_node_id = %target_node_id,
target_rpc_addr = %target_rpc_addr,
)
)]
async fn get_keyspace_diff<S>(
    keyspace_name: String,
    target_node_id: String,
    target_rpc_addr: SocketAddr,
    group: KeyspaceGroup<S>,
    mut client: ReplicationClient<S>,
) -> Result<KeyspaceDiff, GetDiffError>
where
    S: Storage,
{
    let keyspace = group.get_or_create_keyspace(&keyspace_name).await;
    let (last_updated, set) =
        client
            .get_state(keyspace.name())
            .await
            .map_err(|e| GetDiffError {
                cause: e,
                keyspace: keyspace_name.clone(),
                node_id: target_node_id.clone(),
                node_addr: target_rpc_addr,
            })?;

    let (modified, removed) = keyspace.send(Diff(set)).await;

    Ok(KeyspaceDiff {
        keyspace: keyspace_name.clone(),
        modified: modified
            .into_iter()
            .map(|(id, ts)| DocumentMetadata::new(id, ts))
            .collect(),
        removed: removed
            .into_iter()
            .map(|(id, ts)| DocumentMetadata::new(id, ts))
            .collect(),
        last_updated,
    })
}

#[instrument(name = "sync-removed-docs", skip_all)]
/// Starts the synchronisation process of syncing the remote node's keyspace
/// to the current node's keyspace.
///
/// The system begins by requesting the keyspace CRDT and gets the diff between
/// the current CRDT and the remote CRDT.
async fn begin_keyspace_sync<S>(
    ctx: &ReplicationCycleContext<S>,
    keyspace_name: String,
    target_node_id: NodeId,
    target_rpc_addr: SocketAddr,
    removed: DocVec<DocumentMetadata>,
    modified: DocVec<DocumentMetadata>,
) -> Result<(), anyhow::Error>
where
    S: Storage,
{
    let channel = ctx.network.get_or_connect(target_rpc_addr);
    let keyspace = ctx.group.get_or_create_keyspace(&keyspace_name).await;
    let client = ReplicationClient::new(ctx.clock().clone(), channel.clone());

    let progress_tracker = ProgressTracker::default();
    let ctx = PutContext {
        progress: progress_tracker.clone(),
        remote_node_id: target_node_id,
        remote_addr: target_rpc_addr,
        remote_rpc_channel: channel.clone(),
    };

    // The removal task can operate interdependently of the modified handler.
    // If, in the process of handling removals, the modified handler errors,
    // we simply let the removal task continue on as normal.
    let removal_task = tokio::spawn(handle_removals(keyspace.clone(), removed));

    tokio::spawn(handle_modified(client, keyspace, modified, ctx));

    let start = Instant::now();
    let mut watcher = ProgressWatcher::new(progress_tracker, KEYSPACE_SYNC_TIMEOUT);
    let mut interval = interval(Duration::from_millis(250));
    loop {
        interval.tick().await;

        if watcher.has_expired() {
            warn!(total_time = ?start.elapsed(), "Storage task took too long to complete and has been left to run.");
            removal_task.await??;
            return Err(anyhow!(
                "Task timed out and could not be completed. Took {:?}",
                start.elapsed()
            ));
        }

        if watcher.is_done() {
            break;
        }
    }

    removal_task.await??;

    Ok(())
}

#[instrument(name = "sync-modified-docs", skip_all)]
/// Fetches all the documents which have changed since the last state fetch.
///
/// These documents are then persisted and the metadata marked accordingly.
async fn handle_modified<S>(
    mut client: ReplicationClient<S>,
    keyspace: ActorMailbox<KeyspaceActor<S>>,
    modified: DocVec<DocumentMetadata>,
    ctx: PutContext,
) -> Result<(), anyhow::Error>
where
    S: Storage,
{
    let doc_id_chunks = modified
        .chunks(MAX_NUMBER_OF_DOCS_PER_FETCH)
        .map(|entries| entries.iter().map(|doc| doc.id).collect::<Vec<_>>());

    let total = Instant::now();
    let mut total_num_docs = 0;
    for doc_ids in doc_id_chunks {
        let start = Instant::now();
        let docs = client.fetch_docs(keyspace.name(), doc_ids).await?;
        debug!(elapsed = ?start.elapsed(), num_docs = docs.len(), "Fetched docs.");

        total_num_docs += docs.len();

        let msg = MultiSet {
            source: READ_REPAIR_SOURCE_ID,
            docs: DocVec::from_vec(docs),
            ctx: Some(ctx.clone()),
            _marker: PhantomData::<S>::default(),
        };
        keyspace.send(msg).await?;
        ctx.progress.register_progress();
    }
    ctx.progress.set_done();
    info!(elapsed = ?total.elapsed(), num_docs = total_num_docs, "Doc fetching completed.");

    Ok(())
}

#[instrument(name = "sync-removed-docs", skip_all)]
/// Removes the marked documents from the persisted storage and then
/// marks the document metadata as a tombstone.
///
/// This does not remove the metadata of the document entirely, instead the document is marked
/// as deleted along with the main data itself, but we keep a history of the deletes we've made.
async fn handle_removals<S>(
    keyspace: ActorMailbox<KeyspaceActor<S>>,
    mut removed: DocVec<DocumentMetadata>,
) -> Result<(), anyhow::Error>
where
    S: Storage,
{
    if removed.is_empty() {
        return Ok(());
    }

    if removed.len() == 1 {
        let doc = removed.remove(0);
        let msg = Del {
            source: READ_REPAIR_SOURCE_ID,
            doc,
            _marker: PhantomData::<S>::default(),
        };
        keyspace.send(msg).await?;
        return Ok(());
    }

    let msg = MultiDel {
        source: READ_REPAIR_SOURCE_ID,
        docs: removed,
        _marker: PhantomData::<S>::default(),
    };
    keyspace.send(msg).await?;
    Ok(())
}

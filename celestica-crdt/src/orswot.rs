use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::time::Duration;
use std::{cmp, mem};

#[cfg(feature = "rkyv-support")]
use bytecheck::CheckBytes;
#[cfg(feature = "rkyv-support")]
use rkyv::{Archive, Deserialize, Serialize};

use crate::timestamp::HLCTimestamp;

pub type Key = u64;
pub type StateChanges = Vec<(Key, HLCTimestamp)>;

/// The period of time, to remove from the
/// safe timestamp cut off.
///
/// This allows the system to essentially forgive some latency between events.
///
/// This is 1 hour by default.
pub const FORGIVENESS_PERIOD: Duration = if cfg!(test) {
    Duration::from_secs(0)
} else {
    Duration::from_secs(3_600)
};

#[cfg(feature = "rkyv-support")]
#[derive(Debug, thiserror::Error)]
#[error("The set cannot be (de)serialized from the provided set of bytes.")]
/// The set cannot be (de)serialized to or from the byte buffer.
pub struct BadState;

#[derive(Debug, Clone)]
#[repr(C)]
#[cfg_attr(feature = "rkyv-support", derive(Serialize, Deserialize, Archive))]
#[cfg_attr(feature = "rkyv-support", archive(compare(PartialEq)))]
#[cfg_attr(feature = "rkyv-support", archive_attr(derive(CheckBytes)))]
pub struct NodeVersions<const N: usize> {
    nodes_max_stamps: [BTreeMap<u8, HLCTimestamp>; N],
    safe_last_stamps: BTreeMap<u8, HLCTimestamp>,
}

impl<const N: usize> Default for NodeVersions<N> {
    fn default() -> Self {
        let stamps_template = [(); N];

        Self {
            nodes_max_stamps: stamps_template.map(|_| BTreeMap::new()),
            safe_last_stamps: BTreeMap::new(),
        }
    }
}

impl<const N: usize> NodeVersions<N> {
    /// merges the current node versions with another node versions set.
    fn merge(&mut self, other: NodeVersions<N>) {
        let mut nodes = HashSet::new();
        for (source, other_nodes) in other.nodes_max_stamps.into_iter().enumerate() {
            let existing_nodes = &mut self.nodes_max_stamps[source];
            for (node, ts) in other_nodes {
                nodes.insert(node);
                match existing_nodes.entry(node) {
                    Entry::Occupied(mut entry) => {
                        // We have already observed these events at some point from this node.
                        // This means we can no longer trust that this key is in fact still valid.
                        if &ts < entry.get() {
                            continue;
                        }

                        entry.insert(ts);
                    },
                    Entry::Vacant(v) => {
                        v.insert(ts);
                    },
                }
            }
        }

        for node in nodes {
            self.compute_safe_last_stamp(node);
        }
    }

    /// Attempts to update the latest observed timestamp for a given source.
    fn try_update_max_stamp(&mut self, source: usize, ts: HLCTimestamp) -> bool {
        match self.nodes_max_stamps[source].entry(ts.node()) {
            Entry::Occupied(mut entry) => {
                // We have already observed these events at some point from this node.
                // This means we can no longer trust that this key is in fact still valid.
                if &ts < entry.get() {
                    self.compute_safe_last_stamp(ts.node());
                    return false;
                }

                entry.insert(ts);
            },
            Entry::Vacant(v) => {
                v.insert(ts);
            },
        }

        self.compute_safe_last_stamp(ts.node());

        true
    }

    /// Computes the safe observed timestamp based off all known sources.
    fn compute_safe_last_stamp(&mut self, node: u8) {
        let min = self
            .nodes_max_stamps
            .iter()
            .map(|stamps| {
                stamps.get(&node).copied().unwrap_or_else(|| {
                    HLCTimestamp::new(Duration::from_secs(0), 0, node)
                })
            })
            .min();

        if let Some(min) = min {
            let ts = HLCTimestamp::new(
                min.celestica_timestamp().saturating_sub(FORGIVENESS_PERIOD),
                min.counter(),
                min.node(),
            );
            self.safe_last_stamps.insert(node, ts);
        }
    }

    /// Checks if a given timestamp happened before the last observed timestamp.
    fn is_ts_before_last_observed_event(&self, ts: HLCTimestamp) -> bool {
        self.safe_last_stamps
            .get(&ts.node())
            .map(|v| &ts < v)
            .unwrap_or_default()
    }
}

#[derive(Debug, Default, Clone)]
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Serialize, Deserialize, Archive))]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq)))]
#[cfg_attr(feature = "rkyv", archive_attr(derive(CheckBytes)))]
/// A CRDT which supports purging of deleted entry tombstones.
///
/// This implementation is largely based on the Riak DB implementations
/// of CRDTs. This set in particular is built around the [HLCTimestamp]
/// which uses the uniqueness guarantees provided by the timestamp to
/// resolve conflicts.
///
/// Entries can be marked as deleted via the standard `delete` method
/// which internally marks the key as a tombstone.
/// The tombstones can be purged safely once the set has observed other,
/// newer operations from the original node which the entry is tied to.
/// (This is tracked by checking the `node` field of the timestamp.)
///
/// ## Sources
/// Sources are a way of separating two asynchronous operations which both guarantee the order
/// of the operations is correct, but may not be ordered relative to each other.
///
/// I.e. if we have the following sources:
///
/// Source 1: [1, 2, 4, 5]
/// Source 2: [2, 3, 8]
///
/// If we observe the operations from source `2` before source `1` or vice versa, we may miss
/// operations if we assume one source, this is because of our observer pattern.
///
/// Sources allow us to negate this issue while still keeping the observer pattern.
///
/// ## Last write wins conditions
/// If two operations occur at the same effective time, i.e. the `seconds` and `counter` are the
/// same on two timestamps. The timestamp with the largest `node_id` wins.
///
/// Consistency is not guaranteed in the event that two operations with the same timestamp from the
/// *same node* occur on the same key. This means that the node is not generating it's [HLCTimestamp]
/// monotonically which is incorrect.
///
/// The set may converge in it's current state with the above situation, but this is not guaranteed
/// or tested against. It is your responsibility to ensure that timestamps from the same node are
/// monotonic (as ensured by [HLCTimestamp]'s `send` method.)
///
///
/// ## Example
/// ```
/// use std::time::Duration;
/// use celestica_crdt::{OrSWotSet, HLCTimestamp};
///
/// let mut node_a = HLCTimestamp::now(0, 0);
///
/// // Simulating a node begin slightly ahead.
/// let mut node_b = HLCTimestamp::new(node_a.celestica_timestamp() + Duration::from_secs(5), 0, 1);
///
/// // We have only 1 source.
/// let mut node_a_set = OrSWotSet::<1>::default();
/// let mut node_b_set = OrSWotSet::<1>::default();
///
/// // Insert a new key with a new timestamp in set A.
/// node_a_set.insert(1, node_a.send().unwrap());
///
/// // Insert a new entry in set B.
/// node_b_set.insert(2, node_b.send().unwrap());
///
/// // Set A has key `1` removed.
/// node_a_set.delete(1, node_a.send().unwrap());
///
/// // Merging set B with set A and vice versa.
/// // Our sets are now aligned without conflicts.
/// node_b_set.merge(node_a_set.clone());
/// node_a_set.merge(node_b_set.clone());
///
/// // Set A and B should both see that key `1` has been deleted.
/// assert!(node_a_set.get(&1).is_none(), "Key should be correctly removed.");
/// assert!(node_b_set.get(&1).is_none(), "Key should be correctly removed.");
/// ```
pub struct OrSWotSet<const N: usize = 1> {
    entries: BTreeMap<Key, HLCTimestamp>,
    dead: HashMap<Key, HLCTimestamp>,
    versions: NodeVersions<N>,
}

impl<const N: usize> OrSWotSet<N> {
    #[cfg(feature = "rkyv")]
    /// Deserializes a [OrSWotSet] from a array of bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, BadState> {
        let deserialized = rkyv::from_bytes::<Self>(data).map_err(|_| BadState)?;
        Ok(deserialized)
    }

    #[cfg(feature = "rkyv")]
    /// Serializes the set into a buffer of bytes.
    pub fn as_bytes(&self) -> Result<Vec<u8>, BadState> {
        Ok(rkyv::to_bytes::<_, 2048>(self)
            .map_err(|_| BadState)?
            .into_vec())
    }

    /// Calculates the deterministic difference between two sets, returning the
    /// modified keys and the deleted keys.
    ///
    /// This follows the same logic as `set.merge(&other)` but does not modify
    /// the state of the main set.
    ///
    /// NOTE:
    ///     The difference is *not* the symmetric difference between the two sets.
    pub fn diff(&self, other: &OrSWotSet<N>) -> (StateChanges, StateChanges) {
        let mut changes = Vec::new();
        let mut removals = Vec::new();

        for (key, ts) in other.entries.iter() {
            self.check_self_then_insert_to(*key, *ts, &mut changes);
        }

        for (key, ts) in other.dead.iter() {
            self.check_self_then_insert_to(*key, *ts, &mut removals);
        }

        (changes, removals)
    }

    fn check_self_then_insert_to(
        &self,
        key: Key,
        ts: HLCTimestamp,
        values: &mut Vec<(Key, HLCTimestamp)>,
    ) {
        if let Some(existing_insert) = self.entries.get(&key) {
            if existing_insert < &ts {
                values.push((key, ts));
            }
        } else if let Some(existing_delete) = self.dead.get(&key) {
            if existing_delete < &ts {
                values.push((key, ts));
            }
        } else if !self.versions.is_ts_before_last_observed_event(ts) {
            values.push((key, ts))
        }
    }

    /// Merges another set with the current set.
    ///
    /// In this case any conflicts are deterministically resolved via the key's [HLCTimestamp]
    /// any deletes are tracked or ignored depending on this timestamp due to the nature
    /// of the ORSWOT CRDT.
    pub fn merge(&mut self, other: OrSWotSet<N>) {
        let base_entries = other.entries.into_iter().map(|(k, ts)| (k, ts, false));

        let remote_versions = other.versions;
        let mut entries_log = Vec::from_iter(base_entries);
        entries_log.extend(other.dead.into_iter().map(|(k, ts)| (k, ts, true)));

        // It's important we go in time/event order. Otherwise we may incorrectly merge the set.
        entries_log.sort_by_key(|v| v.1);

        let mut old_entries = mem::take(&mut self.entries);

        for (key, ts, is_delete) in entries_log {
            // We've already observed the operation.
            if is_delete && self.versions.is_ts_before_last_observed_event(ts) {
                continue;
            }

            if is_delete {
                if let Some(entry) = self.entries.remove(&key) {
                    if ts < entry {
                        self.entries.insert(key, entry);
                        continue;
                    }
                }

                self.dead
                    .entry(key)
                    .and_modify(|v| {
                        (*v) = cmp::max(*v, ts);
                    })
                    .or_insert_with(|| ts);

                continue;
            }

            let mut timestamp = ts;

            // If our own entry is newer, we use that.
            if let Some(existing_ts) = old_entries.remove(&key) {
                timestamp = cmp::max(timestamp, existing_ts);
            }

            // Have we already marked the document as dead. And if so, is it newer than this op?
            if let Some(deleted_ts) = self.dead.remove(&key) {
                if timestamp < deleted_ts {
                    self.dead.insert(key, deleted_ts);
                    continue;
                }
            }

            self.entries.insert(key, timestamp);
        }

        // The remaining entries in our map are either entries which we need to remove,
        // or entries that the other node is currently missing.
        for (key, ts) in old_entries {
            // We've observed all the events upto and beyond this timestamp.
            // We can rely on this check to see if we delete or keep the entry,
            // as this is a `<` bounds check rather than `<=`. Which means
            // if the entry happens to have been the most recent event observed, it won't
            // be `true` and therefore be kept.
            if remote_versions.is_ts_before_last_observed_event(ts) {
                continue;
            }

            if let Some(deleted) = self.dead.remove(&key) {
                if ts < deleted {
                    self.dead.insert(key, deleted);
                    continue;
                }
            }

            self.entries.insert(key, ts);
        }

        self.versions.merge(remote_versions);
    }

    /// Get an entry from the set.
    ///
    /// If the entry exists it's associated [HLCTimestamp] is returned.
    pub fn get(&self, k: &Key) -> Option<&HLCTimestamp> {
        self.entries.get(k)
    }

    /// Purges and returns any safe to remove tombstone markers from the set.
    ///
    /// This is useful for conserving memory and preventing an infinitely
    /// growing tombstone state.
    pub fn purge_old_deletes(&mut self) -> StateChanges {
        let mut deleted_keys = vec![];
        for (k, stamp) in mem::take(&mut self.dead) {
            if !self.versions.is_ts_before_last_observed_event(stamp) {
                self.dead.insert(k, stamp);
            } else {
                deleted_keys.push((k, stamp));
            }
        }

        deleted_keys
    }

    /// Adds a set of tombstones to the state tombstone list.
    ///
    /// This bypasses the observation timestamp check, which is useful
    /// for replaying purges which may not have been successful.
    ///
    /// WARNING:
    /// If you do not know where to use this, you do not wan't to use this.
    pub fn add_raw_tombstones(&mut self, tombstones: StateChanges) {
        for (key, stamp) in tombstones {
            self.dead.insert(key, stamp);
        }
    }

    /// Checks if the given operation will be applied if it is in fact carried out.
    pub fn will_apply(&self, key: Key, ts: HLCTimestamp) -> bool {
        if self.versions.is_ts_before_last_observed_event(ts) {
            return false;
        }

        if let Some(entry) = self.entries.get(&key) {
            return entry < &ts;
        }

        if let Some(entry) = self.dead.get(&key) {
            return entry < &ts;
        }

        true
    }

    /// Insert a key into the set with a given timestamp.
    ///
    /// If the set has already observed events from the timestamp's
    /// node, this operation is ignored. It is otherwise inserted.
    ///
    /// Returns if the value has actually been inserted/updated
    /// in the set. If `false`, the set's state has not changed.
    pub fn insert(&mut self, k: Key, ts: HLCTimestamp) -> bool {
        self.insert_with_source(0, k, ts)
    }

    /// Insert a key into the set with a given timestamp with a source.
    ///
    /// If the set has already observed events from the timestamp's
    /// node, this operation is ignored. It is otherwise inserted.
    ///
    /// Returns if the value has actually been inserted/updated
    /// in the set. If `false`, the set's state has not changed.
    pub fn insert_with_source(
        &mut self,
        source: usize,
        k: Key,
        ts: HLCTimestamp,
    ) -> bool {
        debug_assert!(source < N);

        let mut has_set = false;

        if !self.versions.try_update_max_stamp(source, ts) {
            return has_set;
        }

        if let Some(deleted_ts) = self.dead.remove(&k) {
            // Our deleted timestamp is newer, so we don't want to adjust our markings.
            if ts < deleted_ts {
                self.dead.insert(k, deleted_ts);
                return has_set;
            }
        }

        self.entries
            .entry(k)
            .and_modify(|v| {
                if *v < ts {
                    has_set = true;
                    (*v) = ts;
                }
            })
            .or_insert_with(|| {
                has_set = true;
                ts
            });

        has_set
    }

    /// Attempts to remove a key from the set with a given timestamp.
    ///
    /// If the set has already observed events from the timestamp's
    /// node, this operation is ignored. It is otherwise inserted.
    ///
    /// Returns if the value has actually been inserted/updated
    /// in the set. If `false`, the set's state has not changed.
    pub fn delete(&mut self, k: Key, ts: HLCTimestamp) -> bool {
        self.delete_with_source(0, k, ts)
    }

    /// Attempts to remove a key from the set with a given timestamp.
    ///
    /// If the set has already observed events from the timestamp's
    /// node, this operation is ignored. It is otherwise inserted.
    ///
    /// Returns if the value has actually been inserted/updated
    /// in the set. If `false`, the set's state has not changed.
    pub fn delete_with_source(
        &mut self,
        source: usize,
        k: Key,
        ts: HLCTimestamp,
    ) -> bool {
        debug_assert!(source < N);

        let mut has_set = false;

        if !self.versions.try_update_max_stamp(source, ts) {
            return has_set;
        }

        if let Some(existing_ts) = self.entries.remove(&k) {
            // Our deleted timestamp is newer, so we don't want to adjust our markings.
            // Inserts *always* win on conflicting timestamps.
            if ts <= existing_ts {
                self.entries.insert(k, existing_ts);
                return has_set;
            }
        }

        self.dead
            .entry(k)
            .and_modify(|v| {
                if *v < ts {
                    has_set = true;
                    (*v) = ts;
                }
            })
            .or_insert_with(|| {
                has_set = true;
                ts
            });

        has_set
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_op_order() {
        let mut node_a = HLCTimestamp::now(0, 0);
        let mut node_b = HLCTimestamp::new(node_a.celestica_timestamp(), 0, 1);

        let mut node_a_set = OrSWotSet::<1>::default();

        let ts_a = node_a.send().unwrap();
        let ts_b = node_b.send().unwrap();

        node_a_set.insert(1, ts_a);
        node_a_set.insert(1, ts_b);

        let retrieved = node_a_set.get(&1);
        assert_eq!(retrieved, Some(&ts_b), "Node B should win the operation.");

        let mut node_a_set = OrSWotSet::<1>::default();
        let mut node_b_set = OrSWotSet::<1>::default();

        node_a_set.insert(1, ts_a);
        node_b_set.insert(1, ts_b);

        node_a_set.merge(node_b_set.clone());
        node_b_set.merge(node_a_set.clone());

        let retrieved = node_a_set.get(&1);
        assert_eq!(
            retrieved,
            Some(&ts_b),
            "Node B should win the operation after merging set A."
        );

        let retrieved = node_b_set.get(&1);
        assert_eq!(
            retrieved,
            Some(&ts_b),
            "Node B should win the operation after merging set B."
        );
    }

    #[test]
    fn test_basic_insert_merge() {
        let mut node_a = HLCTimestamp::now(0, 0);
        let mut node_b = HLCTimestamp::new(node_a.celestica_timestamp(), 0, 1);

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::<1>::default();

        // We add a new set of entries into our set.
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());
        node_a_set.insert(3, node_a.send().unwrap());

        // We create our new state on node b's side.
        let mut node_b_set = OrSWotSet::<1>::default();

        // We add a new set of entries into our set.
        node_b_set.insert(1, node_b.send().unwrap());
        node_b_set.insert(4, node_b.send().unwrap());

        node_a_set.merge(node_b_set);

        assert!(
            node_a_set.dead.is_empty(),
            "Expected no entries to be marked as dead."
        );

        assert!(
            node_a_set.entries.get(&1).is_some(),
            "Expected entry with key 1 to exist."
        );
        assert!(
            node_a_set.entries.get(&2).is_some(),
            "Expected entry with key 2 to exist."
        );
        assert!(
            node_a_set.entries.get(&3).is_some(),
            "Expected entry with key 3 to exist."
        );
        assert!(
            node_a_set.entries.get(&4).is_some(),
            "Expected entry with key 4 to exist."
        );
    }

    #[test]
    fn test_same_time_conflict_convergence() {
        let mut node_a = HLCTimestamp::now(0, 0);
        let mut node_b = HLCTimestamp::new(node_a.celestica_timestamp(), 0, 1);

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::<1>::default();

        // We add a new set of entries into our set.
        // It's important that our `3` key is first here, as it means the counter
        // of the HLC timestamp will mean the delete succeeds.
        node_a_set.insert(3, node_a.send().unwrap());
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());

        // We create our new state on node b's side.
        let mut node_b_set = OrSWotSet::<1>::default();

        // We add a new set of entries into our set.
        // These entries effectively happen at the same time as node A in our test, just because
        // of the execution speed.
        node_b_set.insert(1, node_b.send().unwrap());
        node_b_set.delete(3, node_b.send().unwrap());

        // When merged, the set should mark key `3` as deleted
        // and ignore the insert on the original set.
        node_a_set.merge(node_b_set.clone());

        assert!(
            node_a_set.dead.contains_key(&3),
            "SET A: Expected key 3 to be marked as dead."
        );

        assert!(
            node_a_set.entries.get(&1).is_some(),
            "SET A: Expected entry with key 1 to exist."
        );
        assert!(
            node_a_set.entries.get(&2).is_some(),
            "SET A: Expected entry with key 2 to exist."
        );
        assert!(
            node_a_set.entries.get(&3).is_none(),
            "SET A: Expected entry with key 3 to NOT exist."
        );

        // If we now merge set A with set B. They should align.
        node_b_set.merge(node_a_set);

        assert!(
            node_b_set.dead.contains_key(&3),
            "SET B: Expected key 3 to be marked as dead."
        );

        assert!(
            node_b_set.entries.get(&1).is_some(),
            "SET B: Expected entry with key 1 to exist."
        );
        assert!(
            node_b_set.entries.get(&2).is_some(),
            "SET B: Expected entry with key 2 to exist."
        );
        assert!(
            node_b_set.entries.get(&3).is_none(),
            "SET B: Expected entry with key 3 to NOT exist."
        );
    }

    #[test]
    fn test_basic_delete_merge() {
        let mut node_a = HLCTimestamp::now(0, 0);
        let mut node_b = HLCTimestamp::new(
            node_a.celestica_timestamp() + Duration::from_secs(1),
            0,
            1,
        );

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::<1>::default();

        // We add a new set of entries into our set.
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());
        node_a_set.insert(3, node_a.send().unwrap());

        // We create our new state on node b's side.
        let mut node_b_set = OrSWotSet::<1>::default();

        // We add a new set of entries into our set.
        node_b_set.insert(1, node_b.send().unwrap());
        node_b_set.delete(3, node_b.send().unwrap());

        node_a_set.merge(node_b_set.clone());

        assert!(
            node_a_set.dead.contains_key(&3),
            "Expected key 3 to be marked as dead."
        );

        assert!(
            node_a_set.entries.get(&1).is_some(),
            "Expected entry with key 1 to exist."
        );
        assert!(
            node_a_set.entries.get(&2).is_some(),
            "Expected entry with key 2 to exist."
        );
        assert!(
            node_a_set.entries.get(&3).is_none(),
            "Expected entry with key 3 to NOT exist."
        );
    }

    #[test]
    fn test_purge_delete_merge() {
        let mut node_a = HLCTimestamp::now(0, 0);
        let mut node_b = HLCTimestamp::new(
            node_a.celestica_timestamp() + Duration::from_secs(1),
            0,
            1,
        );

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::<1>::default();

        // We add a new set of entries into our set.
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());
        node_a_set.insert(3, node_a.send().unwrap());

        // We create our new state on node b's side.
        let mut node_b_set = OrSWotSet::<1>::default();

        // We add a new set of entries into our set.
        node_b_set.insert(1, node_b.send().unwrap());
        node_b_set.delete(3, node_b.send().unwrap());

        node_a_set.merge(node_b_set.clone());

        node_a_set.insert(4, node_a.send().unwrap());

        // We must observe another event from node b.
        node_b_set.insert(4, node_b.send().unwrap());
        node_a_set.merge(node_b_set.clone());

        node_a_set.purge_old_deletes();

        assert!(
            node_a_set.dead.is_empty(),
            "Expected dead entries to be empty."
        );

        assert!(
            node_a_set.entries.get(&1).is_some(),
            "Expected entry with key 1 to exist."
        );
        assert!(
            node_a_set.entries.get(&2).is_some(),
            "Expected entry with key 2 to exist."
        );
        assert!(
            node_a_set.entries.get(&3).is_none(),
            "Expected entry with key 3 to NOT exist."
        );
        assert!(
            node_a_set.entries.get(&4).is_some(),
            "Expected entry with key 4 to exist."
        );
    }

    #[test]
    fn test_purge_some_entries() {
        let mut node_a = HLCTimestamp::now(0, 0);
        let mut node_b = HLCTimestamp::new(
            node_a.celestica_timestamp() + Duration::from_secs(1),
            0,
            1,
        );

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::<1>::default();

        // We add a new set of entries into our set.
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());
        node_a_set.insert(3, node_a.send().unwrap());

        std::thread::sleep(Duration::from_millis(1));

        // We create our new state on node b's side.
        let mut node_b_set = OrSWotSet::<1>::default();

        // We add a new set of entries into our set.
        node_b_set.insert(1, node_b.send().unwrap());
        node_b_set.delete(3, node_b.send().unwrap());

        node_a_set.merge(node_b_set.clone());

        node_a_set.insert(4, node_a.send().unwrap());

        // Delete entry 2 from set a.
        node_a_set.delete(2, node_a.send().unwrap());

        // 'observe' a new op happening from node a.
        node_a_set.insert(5, node_a.send().unwrap());

        node_a_set.merge(node_b_set.clone());

        // We expect our deletion of key `2` to be removed, but not key `3`
        node_a_set.purge_old_deletes();

        node_b_set.merge(node_a_set.clone());
        node_a_set.purge_old_deletes();

        assert!(
            node_a_set.dead.get(&3).is_some(),
            "SET A: Expected key 3 to be left in dead set."
        );
        assert!(
            node_a_set.dead.get(&2).is_none(),
            "SET A: Expected key 2 to be purged from dead set."
        );

        assert!(
            node_a_set.entries.get(&1).is_some(),
            "SET A: Expected entry with key 1 to exist."
        );
        assert!(
            node_a_set.entries.get(&2).is_none(),
            "SET A: Expected entry with key 2 to exist."
        );
        assert!(
            node_a_set.entries.get(&3).is_none(),
            "SET A: Expected entry with key 3 to NOT exist."
        );
        assert!(
            node_a_set.entries.get(&4).is_some(),
            "SET A: Expected entry with key 4 to exist."
        );

        assert!(
            node_b_set.dead.get(&3).is_some(),
            "SET B: Expected key 3 to be left in dead set."
        );
        assert!(
            node_b_set.dead.get(&2).is_none(),
            "SET B: Expected key 2 to be purged from dead set."
        );

        assert!(
            node_b_set.entries.get(&1).is_some(),
            "SET B: Expected entry with key 1 to exist."
        );
        assert!(
            node_b_set.entries.get(&2).is_none(),
            "SET B: Expected entry with key 2 to exist."
        );
        assert!(
            node_b_set.entries.get(&3).is_none(),
            "SET B: Expected entry with key 3 to NOT exist."
        );
        assert!(
            node_b_set.entries.get(&4).is_some(),
            "SET B: Expected entry with key 4 to exist."
        );
    }

    #[test]
    fn test_insert_no_op() {
        let mut node_a = HLCTimestamp::now(0, 0);
        let old_ts = node_a.send().unwrap();

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::<1>::default();

        let did_add = node_a_set.insert(1, node_a.send().unwrap());
        assert!(did_add, "Expected entry insert to be added.");

        let did_add = node_a_set.insert(1, old_ts);
        assert!(
            !did_add,
            "Expected entry insert with old timestamp to be ignored"
        );
    }

    #[test]
    fn test_delete_no_op() {
        let mut node_a = HLCTimestamp::now(0, 0);
        let old_ts = node_a.send().unwrap();

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::<1>::default();

        let did_add = node_a_set.insert(1, node_a.send().unwrap());
        assert!(did_add, "Expected entry insert to be added.");

        let did_add = node_a_set.delete(1, old_ts);
        assert!(
            !did_add,
            "Expected entry delete with old timestamp to be ignored"
        );
    }

    #[test]
    fn test_set_diff() {
        let mut node_a = HLCTimestamp::now(0, 0);
        let mut node_b = HLCTimestamp::new(
            node_a.celestica_timestamp() + Duration::from_secs(5),
            0,
            1,
        );

        let mut node_a_set = OrSWotSet::<1>::default();
        let mut node_b_set = OrSWotSet::<1>::default();

        let insert_ts_1 = node_a.send().unwrap();
        node_a_set.insert(1, insert_ts_1);

        let (changed, removed) = OrSWotSet::<1>::default().diff(&node_a_set);
        assert_eq!(
            changed,
            vec![(1, insert_ts_1)],
            "Expected set diff to contain key `1`."
        );
        assert!(
            removed.is_empty(),
            "Expected there to be no difference between sets."
        );

        let delete_ts_3 = node_a.send().unwrap();
        node_a_set.delete(3, delete_ts_3);

        let insert_ts_2 = node_b.send().unwrap();
        node_b_set.insert(2, insert_ts_2);

        let (changed, removed) = node_a_set.diff(&node_b_set);

        assert_eq!(
            changed,
            vec![(2, insert_ts_2)],
            "Expected set a to only be marked as missing key `2`"
        );
        assert!(
            removed.is_empty(),
            "Expected set a to not be missing any delete markers."
        );

        let (changed, removed) = node_b_set.diff(&node_a_set);
        assert_eq!(
            changed,
            vec![(1, insert_ts_1)],
            "Expected set b to have key `1` marked as changed."
        );
        assert_eq!(
            removed,
            vec![(3, delete_ts_3)],
            "Expected set b to have key `3` marked as deleted."
        );
    }

    #[test]
    fn test_set_diff_with_conflicts() {
        let mut node_a = HLCTimestamp::now(0, 0);
        let mut node_b = HLCTimestamp::new(
            node_a.celestica_timestamp() + Duration::from_secs(5),
            0,
            1,
        );

        let mut node_a_set = OrSWotSet::<1>::default();
        let mut node_b_set = OrSWotSet::<1>::default();

        // This should get overriden by node b.
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());

        std::thread::sleep(Duration::from_millis(500));

        let delete_ts_3 = node_a.send().unwrap();
        node_a_set.delete(3, delete_ts_3);

        let insert_ts_2 = node_b.send().unwrap();
        node_b_set.insert(2, insert_ts_2);

        let insert_ts_1 = node_b.send().unwrap();
        node_b_set.insert(1, insert_ts_1);

        let (changed, removed) = node_a_set.diff(&node_b_set);

        assert_eq!(
            changed,
            vec![(1, insert_ts_1), (2, insert_ts_2)],
            "Expected set a to be marked as updating keys `1, 2`"
        );
        assert!(
            removed.is_empty(),
            "Expected set a to not be missing any delete markers."
        );

        let (changed, removed) = node_b_set.diff(&node_a_set);
        assert_eq!(
            changed,
            vec![],
            "Expected set b to have no changed keys marked."
        );
        assert_eq!(
            removed,
            vec![(3, delete_ts_3)],
            "Expected set b to have key `3` marked as deleted."
        );
    }

    #[test]
    fn test_tie_breakers() {
        let node_a = HLCTimestamp::now(0, 0);
        let node_b = HLCTimestamp::new(node_a.celestica_timestamp(), 0, 1);

        let mut node_a_set = OrSWotSet::<1>::default();
        let mut node_b_set = OrSWotSet::<1>::default();

        // This delete conflicts with the insert timestamp.
        // We expect node with the biggest ID to win.
        node_a_set.insert(1, node_a);
        node_b_set.delete(1, node_b);

        let (changed, removed) = node_a_set.diff(&node_b_set);
        assert_eq!(changed, vec![]);
        assert_eq!(removed, vec![(1, node_b)]);

        let (changed, removed) = node_b_set.diff(&node_a_set);
        assert_eq!(changed, vec![]);
        assert_eq!(removed, vec![]);

        node_a_set.merge(node_b_set.clone());
        node_b_set.merge(node_a_set.clone());

        assert!(
            node_a_set.get(&1).is_none(),
            "Set a should no longer have key 1."
        );
        assert!(node_b_set.get(&1).is_none(), "Set b should not have key 1.");

        let (changed, removed) = node_b_set.diff(&node_a_set);
        assert_eq!(changed, vec![]);
        assert_eq!(removed, vec![]);

        let (changed, removed) = node_a_set.diff(&node_b_set);
        assert_eq!(changed, vec![]);
        assert_eq!(removed, vec![]);

        let has_changed = node_a_set.insert(1, node_a);
        assert!(!has_changed, "Set a should not insert the value.");
        let has_changed = node_b_set.insert(1, node_a);
        assert!(
            !has_changed,
            "Set b should not insert the value with node a's timestamp."
        );
        let has_changed = node_a_set.insert(1, node_b);
        assert!(
            has_changed,
            "Set a should insert the value with node b's timestamp."
        );
        let has_changed = node_b_set.insert(1, node_b);
        assert!(has_changed, "Set b should insert the value.");

        let mut node_a_set = OrSWotSet::<1>::default();
        let mut node_b_set = OrSWotSet::<1>::default();

        // This delete conflicts with the insert timestamp.
        // We expect node with the biggest ID to win.
        node_a_set.delete(1, node_a);
        node_b_set.insert(1, node_b);

        let (changed, removed) = node_a_set.diff(&node_b_set);
        assert_eq!(changed, vec![(1, node_b)]);
        assert_eq!(removed, vec![]);

        let (changed, removed) = node_b_set.diff(&node_a_set);
        assert_eq!(changed, vec![]);
        assert_eq!(removed, vec![]);

        node_a_set.merge(node_b_set.clone());
        node_b_set.merge(node_a_set.clone());

        assert!(
            node_a_set.get(&1).is_some(),
            "Set a should no longer have key 1."
        );
        assert!(node_b_set.get(&1).is_some(), "Set b should not have key 1.");
    }

    #[test]
    fn test_multi_source_handling() {
        let mut clock = HLCTimestamp::now(0, 0);
        let mut node_set = OrSWotSet::<1>::default();

        // A basic example of the purging system.
        node_set.insert_with_source(0, 1, clock.send().unwrap());

        node_set.delete_with_source(0, 1, clock.send().unwrap());

        node_set.insert_with_source(0, 3, clock.send().unwrap());
        node_set.insert_with_source(0, 4, clock.send().unwrap());

        // Since we're only using one source here, we should be able to safely purge key `1`.
        let purged = node_set
            .purge_old_deletes()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        assert_eq!(purged, vec![1]);

        let mut node_set = OrSWotSet::<2>::default();

        // Insert a new entry from source `1` and `0`.
        node_set.insert_with_source(0, 1, clock.send().unwrap());
        node_set.insert_with_source(1, 2, clock.send().unwrap());

        // Delete an entry from the set. (Mark it as a tombstone.)
        node_set.delete_with_source(0, 1, clock.send().unwrap());

        // Effectively 'observe' a new set of changes.
        node_set.insert_with_source(0, 3, clock.send().unwrap());
        node_set.insert_with_source(0, 4, clock.send().unwrap());

        // No keys should be purged, because source `1` has not changed it's last
        // observed timestamp, which means the system cannot guarantee that it is safe
        // to remove the entry.
        let purged = node_set.purge_old_deletes();
        assert!(purged.is_empty());

        // Our other source has also now observed a new timestamp.
        node_set.insert_with_source(1, 3, clock.send().unwrap());

        // We should now have successfully removed the key.
        let purged = node_set
            .purge_old_deletes()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        assert_eq!(purged, vec![1]);

        let old_ts = clock.send().unwrap();
        let initial_ts = clock.send().unwrap();

        // Deletes from one source shouldn't affect deletes from the other.
        assert!(node_set.delete_with_source(0, 4, initial_ts));
        assert!(node_set.delete_with_source(1, 3, old_ts));
        assert!(!node_set.delete_with_source(0, 3, old_ts));

        assert!(node_set.insert_with_source(0, 5, initial_ts));
        assert!(node_set.insert_with_source(1, 6, old_ts));
        assert!(!node_set.insert_with_source(0, 5, old_ts));

        assert!(node_set.insert_with_source(0, 6, initial_ts));
        assert!(node_set.delete_with_source(1, 4, clock.send().unwrap()));
        assert!(node_set.delete_with_source(0, 3, clock.send().unwrap()));
        assert!(!node_set.delete_with_source(1, 4, initial_ts));
    }

    #[test]
    fn test_will_apply() {
        let ts = Duration::from_secs(1);
        let mut node_set = OrSWotSet::<1>::default();

        assert!(node_set.will_apply(1, HLCTimestamp::new(ts, 0, 0)));
        node_set.insert(1, HLCTimestamp::new(ts, 0, 0));
        assert!(!node_set.will_apply(1, HLCTimestamp::new(ts, 0, 0)));

        assert!(node_set.will_apply(3, HLCTimestamp::new(Duration::from_secs(3), 0, 0)));
        node_set.delete(3, HLCTimestamp::new(Duration::from_secs(5), 0, 0));
        assert!(!node_set.will_apply(3, HLCTimestamp::new(Duration::from_secs(4), 0, 0)));
    }
}

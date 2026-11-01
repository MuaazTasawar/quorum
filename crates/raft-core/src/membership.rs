use crate::state::NodeId;
use std::collections::BTreeMap;

/// The set of voting members of the cluster, and where to reach each one.
///
/// Every quorum in Raft (committing an entry, winning an election, proving
/// leadership for a read, check-quorum) is a majority of THIS set, not of "all the
/// nodes this process happens to know about". A learner that is still catching up
/// is deliberately not in it: it receives the log but its acknowledgements count
/// for nothing, so a new node that is slow or broken cannot stall the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct Membership {
    members: BTreeMap<NodeId, String>,
}

impl Membership {
    pub fn new(members: impl IntoIterator<Item = (NodeId, String)>) -> Self {
        Self { members: members.into_iter().collect() }
    }

    /// A membership of the given ids with no addresses. Handy where only the
    /// voting arithmetic matters.
    pub fn of_ids(ids: impl IntoIterator<Item = NodeId>) -> Self {
        Self::new(ids.into_iter().map(|id| (id, String::new())))
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.members.contains_key(&id)
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Voting members in ascending id order.
    pub fn ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.members.keys().copied()
    }

    pub fn addr(&self, id: NodeId) -> Option<&str> {
        self.members.get(&id).map(String::as_str)
    }

    /// Every member with its address, in ascending id order.
    pub fn iter(&self) -> impl Iterator<Item = (NodeId, &str)> + '_ {
        self.members.iter().map(|(&id, addr)| (id, addr.as_str()))
    }

    /// How many members make a majority: more than half.
    pub fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }

    /// How many of `ids` are voting members. Ids that are not members (learners,
    /// removed nodes) and repeated ids are ignored.
    pub fn count_members(&self, ids: impl IntoIterator<Item = NodeId>) -> usize {
        let mut seen = std::collections::BTreeSet::new();
        ids.into_iter().filter(|id| self.contains(*id) && seen.insert(*id)).count()
    }

    /// True if `ids` contain a majority of the members.
    pub fn is_quorum(&self, ids: impl IntoIterator<Item = NodeId>) -> bool {
        !self.is_empty() && self.count_members(ids) >= self.quorum()
    }

    /// Applies one configuration change. Adding a member that is already present
    /// keeps it a member and refreshes its address; removing one that is absent
    /// does nothing. Both are harmless on purpose, so replaying a log is always
    /// well defined; the leader refuses such requests before they are ever logged.
    pub fn apply(&mut self, change: &ConfigChange) {
        match change {
            ConfigChange::Add { id, addr } => {
                self.members.insert(*id, addr.clone());
            }
            ConfigChange::Remove { id } => {
                self.members.remove(id);
            }
        }
    }
}

/// One single-server change to the cluster's membership. Raft changes
/// membership one server at a time because any majority of the old configuration
/// and any majority of the new one then overlap, so two leaders can never be
/// elected in the same term by two disjoint majorities (Raft dissertation §4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigChange {
    Add { id: NodeId, addr: String },
    Remove { id: NodeId },
}

impl ConfigChange {
    pub fn id(&self) -> NodeId {
        match self {
            ConfigChange::Add { id, .. } | ConfigChange::Remove { id } => *id,
        }
    }
}

/// The cluster's configuration as the log defines it: a starting membership plus
/// every configuration entry in the log, in order.
///
/// A configuration takes effect the moment its entry is APPENDED to a node's log,
/// not when it commits (Raft dissertation §4.1). So the current membership is the
/// base with ALL recorded changes applied, committed or not, and an entry that is
/// truncated away must stop counting. Keeping the changes as a list (index and
/// change) makes that cheap and exact.
#[derive(Debug, Clone)]
pub struct ConfigHistory {
    base: Membership,
    changes: Vec<(u64, ConfigChange)>,
}

impl ConfigHistory {
    pub fn new(base: Membership) -> Self {
        Self { base, changes: Vec::new() }
    }

    /// Notes that the log entry at `index` carries `change`. Entries must be
    /// recorded in log order.
    pub fn record(&mut self, index: u64, change: ConfigChange) {
        debug_assert!(
            self.changes.last().map_or(true, |(last, _)| *last < index),
            "configuration entries must be recorded in log order"
        );
        self.changes.push((index, change));
    }

    /// Forgets every change at `from_index` or later, mirroring a log truncation.
    pub fn truncate_from(&mut self, from_index: u64) {
        self.changes.retain(|(index, _)| *index < from_index);
    }

    /// The membership in effect: the base with every recorded change applied.
    pub fn current(&self) -> Membership {
        let mut membership = self.base.clone();
        for (_, change) in &self.changes {
            membership.apply(change);
        }
        membership
    }

    /// The membership as it stood once the log entry at `index` had been appended:
    /// the base with every recorded change up to and including `index`. This is what a
    /// snapshot taken at `index` must carry, because the entries that produced it are
    /// about to be discarded.
    pub fn membership_at(&self, index: u64) -> Membership {
        let mut membership = self.base.clone();
        for (_, change) in self.changes.iter().filter(|(i, _)| *i <= index) {
            membership.apply(change);
        }
        membership
    }

    /// Folds every change up to `index` into the base and forgets those entries,
    /// mirroring a log compaction. From here the base is no longer the bootstrap
    /// configuration from the environment but whatever the snapshot recorded.
    pub fn compact_through(&mut self, index: u64) {
        self.base = self.membership_at(index);
        self.changes.retain(|(i, _)| *i > index);
    }

    /// Index of the newest configuration entry in the log, if there is one.
    pub fn last_change_index(&self) -> Option<u64> {
        self.changes.last().map(|(index, _)| *index)
    }

    pub fn len(&self) -> usize {
        self.changes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(id: NodeId) -> ConfigChange {
        ConfigChange::Add { id, addr: format!("node{id}:7000") }
    }

    fn remove(id: NodeId) -> ConfigChange {
        ConfigChange::Remove { id }
    }

    fn ids(membership: &Membership) -> Vec<NodeId> {
        membership.ids().collect()
    }

    #[test]
    fn a_majority_is_more_than_half() {
        assert_eq!(Membership::of_ids([1]).quorum(), 1);
        assert_eq!(Membership::of_ids([1, 2]).quorum(), 2);
        assert_eq!(Membership::of_ids([1, 2, 3]).quorum(), 2);
        assert_eq!(Membership::of_ids([1, 2, 3, 4]).quorum(), 3);
        assert_eq!(Membership::of_ids([1, 2, 3, 4, 5]).quorum(), 3);
    }

    #[test]
    fn only_members_count_toward_a_quorum() {
        let membership = Membership::of_ids([1, 2, 3]);
        assert!(membership.is_quorum([1, 2]));
        assert!(!membership.is_quorum([1]));
        // 8 and 9 are not members (learners or strangers): they add nothing.
        assert!(!membership.is_quorum([1, 8, 9]));
        assert!(membership.is_quorum([1, 3, 9]));
    }

    #[test]
    fn a_repeated_id_is_counted_once() {
        let membership = Membership::of_ids([1, 2, 3]);
        assert!(!membership.is_quorum([2, 2, 2]));
        assert_eq!(membership.count_members([1, 1, 2]), 2);
    }

    #[test]
    fn an_empty_membership_has_no_quorum() {
        let membership = Membership::default();
        assert!(!membership.is_quorum(std::iter::empty()));
    }

    #[test]
    fn adding_and_removing_change_the_set() {
        let mut membership = Membership::of_ids([1, 2, 3]);
        membership.apply(&add(4));
        assert_eq!(ids(&membership), vec![1, 2, 3, 4]);
        assert_eq!(membership.addr(4), Some("node4:7000"));
        membership.apply(&remove(2));
        assert_eq!(ids(&membership), vec![1, 3, 4]);
    }

    #[test]
    fn replaying_a_redundant_change_is_harmless() {
        let mut membership = Membership::of_ids([1, 2, 3]);
        membership.apply(&remove(9));
        assert_eq!(ids(&membership), vec![1, 2, 3]);
        membership.apply(&add(2));
        assert_eq!(ids(&membership), vec![1, 2, 3]);
    }

    #[test]
    fn history_with_no_changes_is_just_the_base() {
        let history = ConfigHistory::new(Membership::of_ids([1, 2, 3]));
        assert_eq!(ids(&history.current()), vec![1, 2, 3]);
        assert_eq!(history.last_change_index(), None);
    }

    #[test]
    fn history_applies_every_recorded_change_in_order() {
        let mut history = ConfigHistory::new(Membership::of_ids([1, 2, 3]));
        history.record(5, add(4));
        history.record(9, remove(1));
        assert_eq!(ids(&history.current()), vec![2, 3, 4]);
        assert_eq!(history.last_change_index(), Some(9));
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn a_removed_then_re_added_member_is_a_member_again() {
        let mut history = ConfigHistory::new(Membership::of_ids([1, 2, 3]));
        history.record(4, remove(3));
        history.record(8, add(3));
        assert_eq!(ids(&history.current()), vec![1, 2, 3]);
    }

    #[test]
    fn truncating_the_log_undoes_the_changes_it_removed() {
        let mut history = ConfigHistory::new(Membership::of_ids([1, 2, 3]));
        history.record(5, add(4));
        history.record(9, add(5));
        history.truncate_from(9);
        assert_eq!(ids(&history.current()), vec![1, 2, 3, 4]);
        history.truncate_from(5);
        assert_eq!(ids(&history.current()), vec![1, 2, 3]);
        assert_eq!(history.last_change_index(), None);
    }

    #[test]
    fn truncating_past_the_last_change_keeps_everything() {
        let mut history = ConfigHistory::new(Membership::of_ids([1, 2, 3]));
        history.record(5, add(4));
        history.truncate_from(6);
        assert_eq!(ids(&history.current()), vec![1, 2, 3, 4]);
    }

    #[test]
    fn membership_at_replays_only_the_changes_up_to_that_index() {
        let mut history = ConfigHistory::new(Membership::of_ids([1, 2, 3]));
        history.record(5, add(4));
        history.record(9, remove(1));
        assert_eq!(ids(&history.membership_at(4)), vec![1, 2, 3]);
        assert_eq!(ids(&history.membership_at(5)), vec![1, 2, 3, 4]);
        assert_eq!(ids(&history.membership_at(8)), vec![1, 2, 3, 4]);
        assert_eq!(ids(&history.membership_at(9)), vec![2, 3, 4]);
    }

    #[test]
    fn compacting_folds_the_old_changes_into_the_base() {
        let mut history = ConfigHistory::new(Membership::of_ids([1, 2, 3]));
        history.record(5, add(4));
        history.record(9, remove(1));
        history.compact_through(5);
        assert_eq!(history.len(), 1, "only the change after the snapshot remains");
        assert_eq!(ids(&history.current()), vec![2, 3, 4], "the answer does not change");
        assert_eq!(ids(&history.membership_at(5)), vec![1, 2, 3, 4]);
        // A truncation of the surviving entry now reverts to the SNAPSHOT's membership.
        history.truncate_from(9);
        assert_eq!(ids(&history.current()), vec![1, 2, 3, 4]);
    }
}

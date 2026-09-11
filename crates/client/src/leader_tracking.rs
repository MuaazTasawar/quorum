use raft_core::NodeId;
use std::collections::HashMap;
use std::sync::Mutex;

/// Tracks which node the client currently believes is the leader, and falls
/// back to round-robin probing the rest of the cluster when that guess is
/// wrong or unknown. A `std::sync::Mutex` is fine here (not `tokio::sync`) -
/// callers only ever hold it for a plain read/write of a small Copy value,
/// never across an `.await`.
pub struct LeaderTracker {
    /// node_id -> metrics API address (e.g. "127.0.0.1:8000")
    nodes: HashMap<NodeId, String>,
    current_guess: Mutex<Option<NodeId>>,
}

impl LeaderTracker {
    pub fn new(nodes: HashMap<NodeId, String>) -> Self {
        Self { nodes, current_guess: Mutex::new(None) }
    }

    /// The address to try first: our current best guess at who the leader is,
    /// if we have one.
    pub fn best_guess(&self) -> Option<(NodeId, String)> {
        let guess = *self.current_guess.lock().unwrap();
        guess.and_then(|id| self.nodes.get(&id).map(|addr| (id, addr.clone())))
    }

    /// Every node EXCEPT the one just tried, in a stable order - used when
    /// we have no leader hint at all and must probe the rest of the cluster.
    pub fn fallback_order(&self, exclude: Option<NodeId>) -> Vec<(NodeId, String)> {
        let mut nodes: Vec<(NodeId, String)> = self
            .nodes
            .iter()
            .filter(|(id, _)| Some(**id) != exclude)
            .map(|(id, addr)| (*id, addr.clone()))
            .collect();
        nodes.sort_by_key(|(id, _)| *id); // deterministic order, easier to reason about in tests/logs
        nodes
    }

    /// Called after a successful request that confirmed which node is leader.
    pub fn record_confirmed_leader(&self, id: NodeId) {
        *self.current_guess.lock().unwrap() = Some(id);
    }

    /// Called when a node tells us it isn't the leader, optionally with a hint
    /// about who is. A hint we recognize becomes the new guess; an unrecognized
    /// or absent hint clears the guess so the next call falls back to probing.
    pub fn record_not_leader(&self, hint: Option<NodeId>) {
        let mut guess = self.current_guess.lock().unwrap();
        *guess = hint.filter(|id| self.nodes.contains_key(id));
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_nodes() -> HashMap<NodeId, String> {
        HashMap::from([
            (1, "127.0.0.1:8001".to_string()),
            (2, "127.0.0.1:8002".to_string()),
            (3, "127.0.0.1:8003".to_string()),
        ])
    }

    #[test]
    fn starts_with_no_guess() {
        let tracker = LeaderTracker::new(sample_nodes());
        assert!(tracker.best_guess().is_none());
    }

    #[test]
    fn records_and_returns_confirmed_leader() {
        let tracker = LeaderTracker::new(sample_nodes());
        tracker.record_confirmed_leader(2);
        assert_eq!(tracker.best_guess(), Some((2, "127.0.0.1:8002".to_string())));
    }

    #[test]
    fn recognized_hint_becomes_new_guess() {
        let tracker = LeaderTracker::new(sample_nodes());
        tracker.record_not_leader(Some(3));
        assert_eq!(tracker.best_guess(), Some((3, "127.0.0.1:8003".to_string())));
    }

    #[test]
    fn unrecognized_hint_clears_guess_instead_of_trusting_it() {
        let tracker = LeaderTracker::new(sample_nodes());
        tracker.record_confirmed_leader(1);
        tracker.record_not_leader(Some(99)); // not a real node in this cluster
        assert!(tracker.best_guess().is_none());
    }

    #[test]
    fn fallback_order_excludes_the_given_node() {
        let tracker = LeaderTracker::new(sample_nodes());
        let fallback = tracker.fallback_order(Some(1));
        let ids: Vec<NodeId> = fallback.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![2, 3]);
    }
}
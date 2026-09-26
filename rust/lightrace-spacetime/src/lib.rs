#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use lightrace_core::CorrelationKey;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionId(pub u128);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RevisionBarrier(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Commit,
    SubscriptionVisibility,
    LocalApply,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObservationOutcome {
    Completed {
        transaction_id: TransactionId,
        revision: RevisionBarrier,
        payload: Vec<u8>,
    },
    FilteredUnrelated,
    IndeterminateTimeout {
        transaction_id: TransactionId,
    },
    Cancelled,
}

#[derive(Clone, Debug)]
pub struct ObserverEntry {
    pub key: CorrelationKey,
    pub expected_tx: TransactionId,
    pub required_revision: RevisionBarrier,
    pub outcome: Option<ObservationOutcome>,
    pub current_phase: Option<Phase>,
}

#[derive(Debug, Default)]
struct State {
    observers: BTreeMap<TransactionId, ObserverEntry>,
    committed_cache: BTreeMap<TransactionId, (RevisionBarrier, Vec<u8>)>,
    outstanding_receipts: BTreeSet<TransactionId>,
    reconciled_receipts: BTreeSet<TransactionId>,
}

#[derive(Clone, Debug, Default)]
pub struct SubscriptionBarrierRegistry {
    state: Arc<Mutex<State>>,
}

impl SubscriptionBarrierRegistry {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    pub fn register_before_send(
        &self,
        key: CorrelationKey,
        tx: TransactionId,
        min_revision: RevisionBarrier,
    ) {
        let mut guard = self.state.lock().expect("poisoned state");
        guard.outstanding_receipts.insert(tx);

        if let Some((rev, payload)) = guard.committed_cache.get(&tx) {
            if *rev >= min_revision {
                let entry = ObserverEntry {
                    key,
                    expected_tx: tx,
                    required_revision: min_revision,
                    outcome: Some(ObservationOutcome::Completed {
                        transaction_id: tx,
                        revision: *rev,
                        payload: payload.clone(),
                    }),
                    current_phase: Some(Phase::LocalApply),
                };
                guard.observers.insert(tx, entry);
                return;
            }
        }

        guard.observers.insert(
            tx,
            ObserverEntry {
                key,
                expected_tx: tx,
                required_revision: min_revision,
                outcome: None,
                current_phase: None,
            },
        );
    }

    pub fn on_commit_receipt(&self, tx: TransactionId, rev: RevisionBarrier, payload: Vec<u8>) {
        let mut guard = self.state.lock().expect("poisoned state");
        guard.committed_cache.insert(tx, (rev, payload.clone()));

        if let Some(entry) = guard.observers.get_mut(&tx) {
            entry.current_phase = Some(Phase::Commit);
            if entry.required_revision <= rev && entry.outcome.is_none() {
                entry.outcome = Some(ObservationOutcome::Completed {
                    transaction_id: tx,
                    revision: rev,
                    payload,
                });
            }
        }
    }

    pub fn on_subscription_delta(
        &self,
        tx: TransactionId,
        key: CorrelationKey,
        rev: RevisionBarrier,
        payload: Vec<u8>,
    ) -> ObservationOutcome {
        let mut guard = self.state.lock().expect("poisoned state");

        if let Some(entry) = guard.observers.get_mut(&tx) {
            if entry.key != key {
                return ObservationOutcome::FilteredUnrelated;
            }
            if rev < entry.required_revision {
                return ObservationOutcome::FilteredUnrelated;
            }

            entry.current_phase = Some(Phase::SubscriptionVisibility);
            let outcome = ObservationOutcome::Completed {
                transaction_id: tx,
                revision: rev,
                payload,
            };
            entry.outcome = Some(outcome.clone());
            outcome
        } else {
            guard.committed_cache.insert(tx, (rev, payload));
            ObservationOutcome::FilteredUnrelated
        }
    }

    pub fn on_local_apply(&self, tx: TransactionId) {
        let mut guard = self.state.lock().expect("poisoned state");
        if let Some(entry) = guard.observers.get_mut(&tx) {
            entry.current_phase = Some(Phase::LocalApply);
        }
    }

    pub fn timeout_after_send(&self, tx: TransactionId) -> ObservationOutcome {
        let mut guard = self.state.lock().expect("poisoned state");
        if let Some(entry) = guard.observers.get_mut(&tx) {
            if entry.outcome.is_none() {
                let outcome = ObservationOutcome::IndeterminateTimeout {
                    transaction_id: tx,
                };
                entry.outcome = Some(outcome.clone());
                return outcome;
            }
        }
        ObservationOutcome::IndeterminateTimeout { transaction_id: tx }
    }

    pub fn reconcile_receipt(&self, tx: TransactionId) -> bool {
        let mut guard = self.state.lock().expect("poisoned state");
        if guard.outstanding_receipts.contains(&tx) {
            guard.reconciled_receipts.insert(tx);
            true
        } else {
            false
        }
    }

    pub fn is_reconciled(&self, tx: TransactionId) -> bool {
        let guard = self.state.lock().expect("poisoned state");
        guard.reconciled_receipts.contains(&tx)
    }

    pub fn get_observer(&self, tx: TransactionId) -> Option<ObserverEntry> {
        let guard = self.state.lock().expect("poisoned state");
        guard.observers.get(&tx).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_key(req: u64) -> CorrelationKey {
        CorrelationKey {
            process_epoch: 1,
            connection_epoch: 1,
            request_id: req,
            session_id: 100,
            session_generation: 1,
            owner_lane: 0,
            command_sequence: req,
            module_generation: 1,
            authority_revision: 1,
        }
    }

    #[test]
    fn register_before_send_and_cache_handoff_cannot_miss_data() {
        let registry = SubscriptionBarrierRegistry::new();
        let key = sample_key(1);
        let tx = TransactionId(1001);

        registry.on_commit_receipt(tx, RevisionBarrier(5), vec![1, 2, 3]);

        registry.register_before_send(key, tx, RevisionBarrier(5));

        let obs = registry.get_observer(tx).expect("observer found");
        assert_eq!(
            obs.outcome,
            Some(ObservationOutcome::Completed {
                transaction_id: tx,
                revision: RevisionBarrier(5),
                payload: vec![1, 2, 3]
            })
        );
        assert_eq!(obs.current_phase, Some(Phase::LocalApply));
    }

    #[test]
    fn commit_visibility_and_local_apply_are_distinct_and_filter_unrelated() {
        let registry = SubscriptionBarrierRegistry::new();
        let key1 = sample_key(10);
        let key2 = sample_key(20);
        let tx1 = TransactionId(2001);
        let tx_unrelated = TransactionId(9999);

        registry.register_before_send(key1, tx1, RevisionBarrier(10));

        let res = registry.on_subscription_delta(tx_unrelated, key2, RevisionBarrier(10), vec![9, 9]);
        assert_eq!(res, ObservationOutcome::FilteredUnrelated);

        let obs = registry.get_observer(tx1).expect("observer found");
        assert_eq!(obs.outcome, None);
        assert_eq!(obs.current_phase, None);

        registry.on_commit_receipt(tx1, RevisionBarrier(10), vec![1, 2]);
        let obs_commit = registry.get_observer(tx1).expect("observer found");
        assert_eq!(obs_commit.current_phase, Some(Phase::Commit));

        registry.on_subscription_delta(tx1, key1, RevisionBarrier(10), vec![1, 2]);
        let obs_vis = registry.get_observer(tx1).expect("observer found");
        assert_eq!(obs_vis.current_phase, Some(Phase::SubscriptionVisibility));

        registry.on_local_apply(tx1);
        let obs_apply = registry.get_observer(tx1).expect("observer found");
        assert_eq!(obs_apply.current_phase, Some(Phase::LocalApply));
    }

    #[test]
    fn timeout_is_indeterminate_and_reconciliation_survives_disconnect() {
        let registry = SubscriptionBarrierRegistry::new();
        let key = sample_key(30);
        let tx = TransactionId(3001);

        registry.register_before_send(key, tx, RevisionBarrier(1));

        let outcome = registry.timeout_after_send(tx);
        assert_eq!(outcome, ObservationOutcome::IndeterminateTimeout { transaction_id: tx });

        assert!(registry.reconcile_receipt(tx));
        assert!(registry.is_reconciled(tx));
    }
}

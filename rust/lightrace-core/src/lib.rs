#![forbid(unsafe_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

pub type ScopeId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CorrelationKey {
    pub process_epoch: u64,
    pub connection_epoch: u64,
    pub request_id: u64,
    pub session_id: u64,
    pub session_generation: u64,
    pub owner_lane: u32,
    pub command_sequence: u64,
    pub module_generation: u64,
    pub authority_revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OperationId {
    slot: u32,
    generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Success(Vec<u8>),
    Rejected(Vec<u8>),
    TransportFailure,
    Cancelled,
    DeadlineExpired,
    ScopeClosed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegisterError {
    CapacityExceeded,
    ScopeClosed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompleteError {
    UnknownOrStaleOperation,
    CorrelationMismatch,
    PayloadTooLarge,
    AlreadyResolved,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScopeError {
    CapacityExceeded,
}

#[derive(Debug, Default)]
struct Slot {
    generation: u64,
    key: Option<CorrelationKey>,
    scope: ScopeId,
    deadline: u64,
    outcome: Option<Outcome>,
    waker: Option<Waker>,
    handle_live: bool,
}

#[derive(Debug)]
struct State {
    slots: Vec<Slot>,
    closed_scopes: Vec<ScopeId>,
    pending: usize,
}

#[derive(Clone, Debug)]
pub struct Registry {
    state: Arc<Mutex<State>>,
    max_payload_bytes: usize,
    max_scopes: usize,
}

impl Registry {
    pub fn new(max_pending: usize, max_payload_bytes: usize, max_scopes: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                slots: (0..max_pending).map(|_| Slot::default()).collect(),
                closed_scopes: Vec::with_capacity(max_scopes),
                pending: 0,
            })),
            max_payload_bytes,
            max_scopes,
        }
    }

    pub fn pending(&self) -> usize {
        self.state.lock().expect("LightRace state poisoned").pending
    }

    pub fn register(
        &self,
        scope: ScopeId,
        key: CorrelationKey,
        deadline: u64,
    ) -> Result<Operation, RegisterError> {
        let mut state = self.state.lock().expect("LightRace state poisoned");
        if state.closed_scopes.contains(&scope) {
            return Err(RegisterError::ScopeClosed);
        }
        let Some(index) = state.slots.iter().position(|slot| slot.key.is_none()) else {
            return Err(RegisterError::CapacityExceeded);
        };
        let slot = &mut state.slots[index];
        slot.generation = slot.generation.wrapping_add(1).max(1);
        slot.key = Some(key);
        slot.scope = scope;
        slot.deadline = deadline;
        slot.outcome = None;
        slot.waker = None;
        slot.handle_live = true;
        let id = OperationId {
            slot: index as u32,
            generation: slot.generation,
        };
        state.pending += 1;
        Ok(Operation {
            id,
            registry: self.clone(),
            resolved: false,
        })
    }

    pub fn complete(
        &self,
        id: OperationId,
        key: CorrelationKey,
        outcome: Outcome,
    ) -> Result<(), CompleteError> {
        if payload_len(&outcome) > self.max_payload_bytes {
            return Err(CompleteError::PayloadTooLarge);
        }
        let wake = {
            let mut state = self.state.lock().expect("LightRace state poisoned");
            let slot = slot_mut(&mut state, id)?;
            if slot.key != Some(key) {
                return Err(CompleteError::CorrelationMismatch);
            }
            resolve_slot(slot, outcome)?
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(())
    }

    pub fn cancel(&self, id: OperationId) -> Result<(), CompleteError> {
        self.resolve_without_key(id, Outcome::Cancelled)
    }

    pub fn expire(&self, id: OperationId, monotonic_now: u64) -> Result<bool, CompleteError> {
        let deadline = {
            let mut state = self.state.lock().expect("LightRace state poisoned");
            slot_mut(&mut state, id)?.deadline
        };
        if monotonic_now < deadline {
            return Ok(false);
        }
        self.resolve_without_key(id, Outcome::DeadlineExpired)?;
        Ok(true)
    }

    pub fn close_scope(&self, scope: ScopeId) -> Result<usize, ScopeError> {
        let (wakes, resolved) = {
            let mut state = self.state.lock().expect("LightRace state poisoned");
            if !state.closed_scopes.contains(&scope) {
                if state.closed_scopes.len() == self.max_scopes {
                    return Err(ScopeError::CapacityExceeded);
                }
                state.closed_scopes.push(scope);
            }
            let mut wakes = Vec::new();
            let mut resolved = 0;
            for slot in &mut state.slots {
                if slot.key.is_some() && slot.scope == scope && slot.outcome.is_none() {
                    slot.outcome = Some(Outcome::ScopeClosed);
                    resolved += 1;
                    if let Some(waker) = slot.waker.take() {
                        wakes.push(waker);
                    }
                }
            }
            (wakes, resolved)
        };
        for waker in wakes {
            waker.wake();
        }
        Ok(resolved)
    }

    fn resolve_without_key(&self, id: OperationId, outcome: Outcome) -> Result<(), CompleteError> {
        let wake = {
            let mut state = self.state.lock().expect("LightRace state poisoned");
            resolve_slot(slot_mut(&mut state, id)?, outcome)?
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(())
    }

    fn poll(&self, id: OperationId, cx: &mut Context<'_>) -> Poll<Outcome> {
        let mut state = self.state.lock().expect("LightRace state poisoned");
        let Ok(slot) = slot_mut(&mut state, id) else {
            return Poll::Ready(Outcome::Cancelled);
        };
        if let Some(outcome) = slot.outcome.take() {
            slot.key = None;
            slot.waker = None;
            slot.handle_live = false;
            state.pending = state.pending.saturating_sub(1);
            return Poll::Ready(outcome);
        }
        if slot
            .waker
            .as_ref()
            .is_none_or(|old| !old.will_wake(cx.waker()))
        {
            slot.waker = Some(cx.waker().clone());
        }
        // Outcome and waker are checked/installed under the same lock, so a
        // callback cannot land between the check and registration.
        Poll::Pending
    }

    fn abandon(&self, id: OperationId) {
        let mut state = self.state.lock().expect("LightRace state poisoned");
        let Ok(slot) = slot_mut(&mut state, id) else {
            return;
        };
        if slot.handle_live {
            slot.key = None;
            slot.outcome = None;
            slot.waker = None;
            slot.handle_live = false;
            state.pending = state.pending.saturating_sub(1);
        }
    }
}

fn payload_len(outcome: &Outcome) -> usize {
    match outcome {
        Outcome::Success(v) | Outcome::Rejected(v) => v.len(),
        _ => 0,
    }
}

fn slot_mut(state: &mut State, id: OperationId) -> Result<&mut Slot, CompleteError> {
    let Some(slot) = state.slots.get_mut(id.slot as usize) else {
        return Err(CompleteError::UnknownOrStaleOperation);
    };
    if slot.generation != id.generation || slot.key.is_none() {
        return Err(CompleteError::UnknownOrStaleOperation);
    }
    Ok(slot)
}

fn resolve_slot(slot: &mut Slot, outcome: Outcome) -> Result<Option<Waker>, CompleteError> {
    if slot.outcome.is_some() {
        return Err(CompleteError::AlreadyResolved);
    }
    slot.outcome = Some(outcome);
    Ok(slot.waker.take())
}

pub struct Operation {
    id: OperationId,
    registry: Registry,
    resolved: bool,
}

impl Operation {
    pub fn id(&self) -> OperationId {
        self.id
    }
}

impl Future for Operation {
    type Output = Outcome;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let poll = self.registry.poll(self.id, cx);
        if poll.is_ready() {
            self.resolved = true;
        }
        poll
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        if !self.resolved {
            self.registry.abandon(self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    fn key(request_id: u64) -> CorrelationKey {
        CorrelationKey {
            process_epoch: 1,
            connection_epoch: 2,
            request_id,
            session_id: 3,
            session_generation: 4,
            owner_lane: 5,
            command_sequence: 6,
            module_generation: 7,
            authority_revision: 8,
        }
    }

    fn counter_waker(counter: Arc<AtomicUsize>) -> Waker {
        struct Counter(Arc<AtomicUsize>);
        impl Wake for Counter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        Waker::from(Arc::new(Counter(counter)))
    }

    fn poll_once(op: &mut Operation, waker: &Waker) -> Poll<Outcome> {
        let mut cx = Context::from_waker(waker);
        Pin::new(op).poll(&mut cx)
    }

    #[test]
    fn callback_before_first_poll_is_retained() {
        let registry = Registry::new(1, 16, 4);
        let mut op = registry.register(9, key(1), 100).unwrap();
        registry
            .complete(op.id(), key(1), Outcome::Success(b"ok".to_vec()))
            .unwrap();
        let w = counter_waker(Arc::new(AtomicUsize::new(0)));
        assert_eq!(
            poll_once(&mut op, &w),
            Poll::Ready(Outcome::Success(b"ok".to_vec()))
        );
        assert_eq!(registry.pending(), 0);
    }

    #[test]
    fn wake_registration_has_no_lost_window_and_completion_is_exactly_once() {
        let registry = Registry::new(1, 16, 4);
        let mut op = registry.register(9, key(2), 100).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let w = counter_waker(count.clone());
        assert_eq!(poll_once(&mut op, &w), Poll::Pending);
        registry
            .complete(op.id(), key(2), Outcome::Success(vec![]))
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(
            registry.complete(op.id(), key(2), Outcome::Success(vec![])),
            Err(CompleteError::AlreadyResolved)
        );
    }

    #[test]
    fn cancel_complete_race_and_reused_slot_are_generation_fenced() {
        let registry = Registry::new(1, 16, 4);
        let mut first = registry.register(9, key(3), 100).unwrap();
        let stale = first.id();
        registry.cancel(stale).unwrap();
        assert_eq!(
            registry.complete(stale, key(3), Outcome::Success(vec![])),
            Err(CompleteError::AlreadyResolved)
        );
        let w = counter_waker(Arc::new(AtomicUsize::new(0)));
        assert_eq!(poll_once(&mut first, &w), Poll::Ready(Outcome::Cancelled));
        let second = registry.register(9, key(4), 100).unwrap();
        assert_ne!(stale, second.id());
        assert_eq!(
            registry.complete(stale, key(3), Outcome::Success(vec![])),
            Err(CompleteError::UnknownOrStaleOperation)
        );
    }

    #[test]
    fn capacity_payload_deadline_and_scope_shutdown_are_bounded() {
        let registry = Registry::new(2, 3, 2);
        let first = registry.register(10, key(5), 50).unwrap();
        let mut second = registry.register(10, key(6), 50).unwrap();
        assert!(matches!(
            registry.register(11, key(7), 50),
            Err(RegisterError::CapacityExceeded)
        ));
        assert_eq!(
            registry.complete(first.id(), key(5), Outcome::Success(vec![0; 4])),
            Err(CompleteError::PayloadTooLarge)
        );
        assert_eq!(registry.expire(first.id(), 49), Ok(false));
        assert_eq!(registry.expire(first.id(), 50), Ok(true));
        assert_eq!(registry.close_scope(10), Ok(1));
        let w = counter_waker(Arc::new(AtomicUsize::new(0)));
        assert_eq!(
            poll_once(&mut second, &w),
            Poll::Ready(Outcome::ScopeClosed)
        );
        assert!(matches!(
            registry.register(10, key(8), 99),
            Err(RegisterError::ScopeClosed)
        ));
    }

    #[test]
    fn full_correlation_key_is_required() {
        let registry = Registry::new(1, 8, 1);
        let op = registry.register(1, key(9), 100).unwrap();
        assert_eq!(
            registry.complete(op.id(), key(10), Outcome::Success(vec![])),
            Err(CompleteError::CorrelationMismatch)
        );
    }

    #[test]
    fn cancel_and_callback_race_has_one_terminal_winner() {
        for request in 100..200 {
            let registry = Registry::new(1, 8, 1);
            let mut op = registry.register(1, key(request), 100).unwrap();
            let id = op.id();
            let barrier = Arc::new(Barrier::new(3));
            let cancel_registry = registry.clone();
            let cancel_barrier = barrier.clone();
            let cancel = std::thread::spawn(move || {
                cancel_barrier.wait();
                cancel_registry.cancel(id)
            });
            let complete_registry = registry.clone();
            let complete_barrier = barrier.clone();
            let complete = std::thread::spawn(move || {
                complete_barrier.wait();
                complete_registry.complete(id, key(request), Outcome::Success(vec![1]))
            });
            barrier.wait();
            let cancel_result = cancel.join().unwrap();
            let complete_result = complete.join().unwrap();
            assert_ne!(cancel_result.is_ok(), complete_result.is_ok());
            let w = counter_waker(Arc::new(AtomicUsize::new(0)));
            assert!(matches!(
                poll_once(&mut op, &w),
                Poll::Ready(Outcome::Cancelled | Outcome::Success(_))
            ));
        }
    }
}

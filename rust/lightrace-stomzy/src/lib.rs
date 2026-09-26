#![forbid(unsafe_code)]

use lightrace_core::{
    CompleteError, CorrelationKey, Operation, Outcome, RegisterError, Registry, ScopeId,
};
use stomzy_runtime::command_execution::OwnedAsyncRequest;

pub struct PendingCommand {
    pub operation: Operation,
    pub key: CorrelationKey,
    pub request: OwnedAsyncRequest,
}

#[derive(Clone)]
pub struct StomzyLightRace {
    registry: Registry,
    process_epoch: u64,
    connection_epoch: u64,
    owner_lane: u32,
}

impl StomzyLightRace {
    pub fn new(
        max_pending: usize,
        max_payload: usize,
        max_scopes: usize,
        process_epoch: u64,
        connection_epoch: u64,
        owner_lane: u32,
    ) -> Self {
        Self {
            registry: Registry::new(max_pending, max_payload, max_scopes),
            process_epoch,
            connection_epoch,
            owner_lane,
        }
    }
    pub fn register(
        &self,
        scope: ScopeId,
        request: OwnedAsyncRequest,
        deadline: u64,
    ) -> Result<PendingCommand, RegisterError> {
        let key = CorrelationKey {
            process_epoch: self.process_epoch,
            connection_epoch: self.connection_epoch,
            request_id: request.request_id,
            session_id: request.session,
            session_generation: request.required.session_generation,
            owner_lane: self.owner_lane,
            command_sequence: request.sequence,
            module_generation: request.required.module_generation,
            authority_revision: request.required.world_revision,
        };
        let operation = self.registry.register(scope, key, deadline)?;
        Ok(PendingCommand {
            operation,
            key,
            request,
        })
    }
    pub fn complete(
        &self,
        pending: &PendingCommand,
        key: CorrelationKey,
        payload: Vec<u8>,
    ) -> Result<(), CompleteError> {
        self.registry
            .complete(pending.operation.id(), key, Outcome::Success(payload))
    }
    pub fn close_scope(&self, scope: ScopeId) -> Result<usize, lightrace_core::ScopeError> {
        self.registry.close_scope(scope)
    }
    pub fn pending(&self) -> usize {
        self.registry.pending()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use stomzy_runtime::command_execution::AuthorityRevision;

    fn request() -> OwnedAsyncRequest {
        OwnedAsyncRequest {
            request_id: 1,
            sequence: 2,
            session: 3,
            command_entity: 4,
            arguments: Arc::new([]),
            required: AuthorityRevision {
                session_generation: 5,
                permission_revision: 6,
                module_generation: 7,
                registry_revision: 8,
                world_revision: 9,
            },
            payload: Arc::new([]),
        }
    }

    #[test]
    fn full_correlation_and_scope_cancellation_preserve_owned_request() {
        let adapter = StomzyLightRace::new(1, 16, 1, 10, 11, 12);
        let pending = adapter.register(13, request(), 100).unwrap();
        assert_eq!(pending.key.session_generation, 5);
        let mut stale = pending.key;
        stale.module_generation += 1;
        assert_eq!(
            adapter.complete(&pending, stale, vec![]),
            Err(CompleteError::CorrelationMismatch)
        );
        assert_eq!(adapter.close_scope(13).unwrap(), 1);
        assert_eq!(pending.request.required.permission_revision, 6);
    }
}

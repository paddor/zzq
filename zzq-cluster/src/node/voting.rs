//! The replication core of a node, absent while it recovers lost state.
//! Without a core the node votes for nothing and answers as not normal.

use std::time::Duration;

use crate::driver::{Action, DriverError, ReplicaDriver};
use crate::wire::Control;
use crate::{
    Admission, Digest, InstallTicket, JournalGeneration, LogSource, NodeId, NormalReplica, Prefix,
    PreparedOperation, PromiseTicket, ReplicationError, Scope, SelectedView, StartView,
    WriteTicket,
};

const NOT_NORMAL: DriverError = DriverError::Replication(ReplicationError::NotNormal);

#[derive(Debug)]
pub(super) struct Driver {
    inner: Option<ReplicaDriver>,
    /// The scope while there is no core: the configuration's, at the
    /// highest view seen.
    scope: Scope,
}

impl Driver {
    pub(super) const fn new(inner: ReplicaDriver) -> Driver {
        Driver {
            scope: inner.scope(),
            inner: Some(inner),
        }
    }

    /// Drops the core, keeping its scope.
    pub(super) fn clear(&mut self) {
        self.scope = self.scope();
        self.inner = None;
    }

    pub(super) const fn normal(&self) -> Option<&NormalReplica> {
        match &self.inner {
            Some(d) => d.normal(),
            None => None,
        }
    }

    pub(super) const fn scope(&self) -> Scope {
        match &self.inner {
            Some(d) => d.scope(),
            None => self.scope,
        }
    }

    pub(super) fn intact_state(&self) -> Option<crate::RecoveredState> {
        self.inner.as_ref()?.intact_state()
    }

    pub(super) fn installation_ticket(&self) -> Option<InstallTicket> {
        self.inner.as_ref()?.installation_ticket()
    }

    pub(super) fn next_deadline(&self) -> Option<Duration> {
        self.inner.as_ref()?.next_deadline()
    }

    pub(super) fn history_sources(&self) -> [Option<LogSource>; 3] {
        self.inner
            .as_ref()
            .map_or([None; 3], ReplicaDriver::history_sources)
    }

    pub(super) fn poll(&mut self, now: Duration) -> Result<Option<Action>, DriverError> {
        self.inner.as_mut().map_or(Ok(None), |d| d.poll(now))
    }

    pub(super) fn receive(
        &mut self,
        from: NodeId,
        message: Control,
        now: Duration,
    ) -> Result<Option<StartView>, DriverError> {
        self.inner
            .as_mut()
            .map_or(Ok(None), |d| d.receive(from, message, now))
    }

    pub(super) fn prepare(
        &mut self,
        from: NodeId,
        scope: Scope,
        operations: &[PreparedOperation],
        now: Duration,
    ) -> Result<Admission, DriverError> {
        self.get()?.prepare(from, scope, operations, now)
    }

    pub(super) fn select(
        &mut self,
        lookup: impl FnMut(LogSource, Prefix) -> Option<Digest>,
    ) -> Result<SelectedView, DriverError> {
        self.get()?.select(lookup)
    }

    pub(super) fn complete_promise(&mut self, ticket: PromiseTicket) -> Result<(), DriverError> {
        self.get()?.complete_promise(ticket)
    }

    pub(super) fn complete_durable_write(
        &mut self,
        ticket: WriteTicket,
        now: Duration,
    ) -> Result<(), DriverError> {
        self.get()?.complete_durable_write(ticket, now)
    }

    pub(super) fn apply_through(&mut self, through: Prefix) -> Result<(), DriverError> {
        self.get()?.apply_through(through)
    }

    pub(super) fn history_progress(&mut self, now: Duration) -> Result<(), DriverError> {
        self.inner
            .as_mut()
            .map_or(Ok(()), |d| d.history_progress(now))
    }

    pub(super) fn begin_primary_install(
        &mut self,
        generation: JournalGeneration,
    ) -> Result<InstallTicket, DriverError> {
        self.get()?.begin_primary_install(generation)
    }

    pub(super) fn begin_backup_install(
        &mut self,
        from: NodeId,
        start: StartView,
        generation: JournalGeneration,
        lookup: impl FnMut(LogSource, Prefix) -> Option<Digest>,
    ) -> Result<InstallTicket, DriverError> {
        self.get()?
            .begin_backup_install(from, start, generation, lookup)
    }

    pub(super) fn validate_install_suffix(
        &mut self,
        operations: &[PreparedOperation],
    ) -> Result<(), DriverError> {
        self.get()?.validate_install_suffix(operations)
    }

    pub(super) fn complete_installation(
        &mut self,
        ticket: &InstallTicket,
        applied: Prefix,
        now: Duration,
    ) -> Result<(), DriverError> {
        self.get()?.complete_installation(*ticket, applied, now)
    }

    pub(super) fn complete_abandon(
        &mut self,
        ticket: &InstallTicket,
        now: Duration,
    ) -> Result<(), DriverError> {
        self.get()?.complete_abandon(*ticket, now)
    }

    fn get(&mut self) -> Result<&mut ReplicaDriver, DriverError> {
        self.inner.as_mut().ok_or(NOT_NORMAL)
    }
}

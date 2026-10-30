//! The node's part of an exit dump: its view, history and log, so that it
//! votes again after a restart instead of recovering from the others.
//!
//! ```text
//! dump  = magic:8 group:16 epoch:u64 digest:32 node:16
//!         promised:u64 last_normal:u64 accepted:prefix committed:prefix
//!         app:prefix base:prefix count:u32 op* check:32
//! op    = original_view:u64 kind:u16 len:u32 body
//! prefix = op:u64 digest:32
//! ```
//!
//! `check` hashes everything before it. A dump is good for one start: the
//! node votes with it, so a later crash must not bring it back.

use std::time::Duration;

use bytes::{Bytes, BytesMut};

use super::log::{Entry, Log};
use super::voting::Driver;
use super::{Node, NodeConfig, NodeError};
use crate::driver::ReplicaDriver;
use crate::integrity::hash;
use crate::operation::{CanonicalOperation, OperationKind, canonical_body_digest};
use crate::{
    Configuration, Digest, FrozenLog, OpNumber, Prefix, RecoveredState, Scope, ViewChange,
};

const MAGIC: &[u8; 8] = b"ZZQNODE1";
const CHECK_CONTEXT: &str = "zzq cluster node dump v1";
const PREFIX_BYTES: usize = 8 + 32;
const HEAD_BYTES: usize = 8 + 16 + 8 + 32 + 16 + 8 + 8 + 4 * PREFIX_BYTES + 4;
const OP_HEAD_BYTES: usize = 8 + 2 + 4;

/// Why a dump could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DumpError {
    /// Not a node dump, cut short, or its checksum does not match.
    #[error("malformed node dump")]
    Malformed,
    /// A dump of another cluster or node.
    #[error("node dump of another cluster or node")]
    Foreign,
    /// Its ops do not chain to the history it names.
    #[error("node dump history does not chain")]
    History,
}

/// What a dump holds, before it becomes a node.
struct Parsed {
    /// The configuration it ran: the given one or a forced epoch of it.
    configuration: Configuration,
    promised: u64,
    last_normal: u64,
    accepted: Prefix,
    committed: Prefix,
    app: Prefix,
    log: Log,
}

impl Node {
    /// The node's part of an exit dump, and the op its application must
    /// dump through. Call once the application applied every op handed to
    /// it and takes no more input. None while the node recovers or
    /// installs a view: it then restarts as [`Node::recover`] would.
    pub fn export(&self) -> Option<(Bytes, OpNumber)> {
        if self.recovering.is_some() {
            return None;
        }
        let state = self.driver.intact_state()?;
        let app = if self.own_from.is_some() {
            self.tip
        } else {
            self.log.prefix_at(self.app_applied).unwrap_or(self.tip)
        };
        let scope = self.config.configuration.scope();
        let mut b = BytesMut::with_capacity(HEAD_BYTES + 64);
        b.extend_from_slice(MAGIC);
        b.extend_from_slice(scope.group_id.as_bytes());
        b.extend_from_slice(&scope.configuration_epoch.to_be_bytes());
        b.extend_from_slice(scope.configuration_digest.as_bytes());
        b.extend_from_slice(self.config.local.as_bytes());
        b.extend_from_slice(&state.scope.view.to_be_bytes());
        b.extend_from_slice(&state.log.last_normal_view.to_be_bytes());
        for p in [
            state.log.accepted,
            state.log.committed,
            app,
            self.log.base(),
        ] {
            put_prefix(&mut b, p);
        }
        let count = self.log.iter_after(self.log.base().op).count();
        b.extend_from_slice(&u32::try_from(count).ok()?.to_be_bytes());
        for e in self.log.iter_after(self.log.base().op) {
            b.extend_from_slice(&e.prepared.original_view.to_be_bytes());
            b.extend_from_slice(&(e.kind as u16).to_be_bytes());
            b.extend_from_slice(&u32::try_from(e.body.len()).ok()?.to_be_bytes());
            b.extend_from_slice(&e.body);
        }
        let check = hash(CHECK_CONTEXT, &b);
        b.extend_from_slice(check.as_bytes());
        Some((b.freeze(), app.op))
    }

    /// A node from its exit dump: it votes again with the history it had,
    /// in the next election, and its application starts from the state it
    /// dumped through the op [`Node::export`] returned.
    ///
    /// # Errors
    ///
    /// [`NodeError::Dump`] for a malformed or foreign dump;
    /// [`NodeError::Driver`] if the core rejects the history.
    pub fn restore(base: NodeConfig, dump: &[u8], now: Duration) -> Result<Node, NodeError> {
        let parsed = parse(&base, dump)?;
        let config = NodeConfig {
            configuration: parsed.configuration,
            ..base
        };
        let scope = config.configuration.scope();
        let recovered = RecoveredState {
            scope: Scope {
                view: parsed.promised,
                ..scope
            },
            log: FrozenLog {
                last_normal_view: parsed.last_normal,
                accepted: parsed.accepted,
                committed: parsed.committed,
            },
        };
        let changing = ViewChange::recover_intact(
            config.configuration,
            config.local,
            super::fresh_generation(),
            recovered,
            config.pipeline,
        )?;
        let driver = ReplicaDriver::from_view_change(changing, now, config.timing)?;
        let mut node = Node::bootstrap(config, now)?;
        node.epochs.base = base.configuration;
        node.driver = Driver::new(driver);
        node.role = super::Role::Electing;
        node.own_from = None;
        node.log = parsed.log;
        node.committed = parsed.committed;
        node.emitted = parsed.app.op;
        node.app_applied = parsed.app.op;
        node.tip = parsed.app;
        node.events.clear();
        node.update_role();
        node.events.clear();
        Ok(node)
    }
}

fn put_prefix(b: &mut BytesMut, p: Prefix) {
    b.extend_from_slice(&p.op.0.to_be_bytes());
    b.extend_from_slice(p.digest.as_bytes());
}

/// The configuration a dump's head names: `config`'s, or a forced epoch
/// of it, for this node.
fn read_configuration(r: &mut Reader<'_>, config: &NodeConfig) -> Result<Configuration, DumpError> {
    let scope = config.configuration.scope();
    let group = r.take(16)?;
    let epoch = r.u64()?;
    let digest = Digest::from_bytes(r.take(32)?.try_into().map_err(|_| DumpError::Malformed)?);
    let local = r.take(16)?;
    if group != scope.group_id.as_bytes() || local != config.local.as_bytes() {
        return Err(DumpError::Foreign);
    }
    super::epoch::named(&config.configuration, epoch, digest).ok_or(DumpError::Foreign)
}

/// The configuration epoch a dump for `config` ran.
pub(super) fn epoch_of(config: &NodeConfig, dump: &[u8]) -> Result<u64, DumpError> {
    Ok(parse(config, dump)?
        .configuration
        .scope()
        .configuration_epoch)
}

/// Reads a dump for `config`, checking its checksum and history.
fn parse(config: &NodeConfig, dump: &[u8]) -> Result<Parsed, DumpError> {
    let (body, check) = dump.split_last_chunk::<32>().ok_or(DumpError::Malformed)?;
    if hash(CHECK_CONTEXT, body).as_bytes() != check {
        return Err(DumpError::Malformed);
    }
    let mut r = Reader(body);
    if r.take(8)? != MAGIC {
        return Err(DumpError::Malformed);
    }
    let configuration = read_configuration(&mut r, config)?;
    let promised = r.u64()?;
    let last_normal = r.u64()?;
    let accepted = r.prefix()?;
    let committed = r.prefix()?;
    let app = r.prefix()?;
    let base = r.prefix()?;
    let log = read_ops(&mut r, base, &configuration, config)?;
    if !r.0.is_empty() {
        return Err(DumpError::Malformed);
    }
    let held = |p: Prefix| p.op <= base.op || log.prefix_at(p.op) == Some(p);
    if log.last() != accepted || !held(committed) || !held(app) || committed.op > accepted.op {
        return Err(DumpError::History);
    }
    Ok(Parsed {
        configuration,
        promised,
        last_normal,
        accepted,
        committed,
        app,
        log,
    })
}

/// The dumped ops after `base`, chained anew from it.
fn read_ops(
    r: &mut Reader<'_>,
    base: Prefix,
    configuration: &Configuration,
    config: &NodeConfig,
) -> Result<Log, DumpError> {
    let scope = configuration.scope();
    let count = r.u32()?;
    if (count as usize).saturating_mul(OP_HEAD_BYTES) > r.0.len() {
        return Err(DumpError::Malformed);
    }
    let mut log = Log::new(base, config.log);
    for _ in 0..count {
        let original_view = r.u64()?;
        let kind = OperationKind::try_from(r.u16()?).map_err(|_| DumpError::Malformed)?;
        let len = r.u32()? as usize;
        let body = Bytes::copy_from_slice(r.take(len)?);
        let previous = log.last();
        let digest = canonical_body_digest(&body);
        let entry = Entry::new(
            CanonicalOperation {
                group_id: scope.group_id,
                configuration_epoch: scope.configuration_epoch,
                original_view,
                op_number: previous.op.0 + 1,
                previous_digest: previous.digest,
                kind,
                body: &body,
            },
            body.clone(),
            digest,
        );
        // Kept whole: the caps bound what was dumped.
        log.push(entry, OpNumber(0), OpNumber(0));
    }
    Ok(log)
}

/// Reads big-endian fields, failing on a short input.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DumpError> {
        let (head, rest) = self.0.split_at_checked(n).ok_or(DumpError::Malformed)?;
        self.0 = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DumpError> {
        self.take(N)?.try_into().map_err(|_| DumpError::Malformed)
    }

    fn u16(&mut self) -> Result<u16, DumpError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, DumpError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, DumpError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn prefix(&mut self) -> Result<Prefix, DumpError> {
        Ok(Prefix {
            op: OpNumber(self.u64()?),
            digest: Digest::from_bytes(self.array()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::Timing;
    use crate::node::LogLimits;
    use crate::wire::WireLimits;
    use crate::{Configuration, GroupId, NodeId, PipelineLimits};

    fn config(local: u8, digest: u8) -> NodeConfig {
        NodeConfig {
            configuration: Configuration::new(
                GroupId::from_bytes([7; 16]),
                1,
                Digest::from_bytes([digest; 32]),
                [1, 2, 3].map(|i| NodeId::from_bytes([i; 16])),
            )
            .unwrap(),
            local: NodeId::from_bytes([local; 16]),
            timing: Timing {
                heartbeat: Duration::from_millis(10),
                primary_timeout: Duration::from_millis(100),
                retransmit: Duration::from_millis(20),
                election_timeout: Duration::from_millis(150),
                max_election_timeout: Duration::from_secs(1),
            },
            pipeline: PipelineLimits {
                max_operations: 64,
                max_body_bytes: 1 << 20,
            },
            log: LogLimits::default(),
            wire: WireLimits::for_transfer(64, 1 << 20).unwrap(),
            fingerprint: 4,
        }
    }

    /// The dump of a fresh leader that proposed three ops.
    fn dump() -> (Bytes, OpNumber) {
        let mut node = Node::bootstrap(config(1, 8), Duration::ZERO).unwrap();
        // As if a follower acked: it leads.
        node.lease.heard_at = Some(Duration::ZERO);
        node.update_role();
        for n in 0..3u8 {
            node.propose(Bytes::from(vec![n; 5]), Duration::ZERO)
                .unwrap();
        }
        node.export().unwrap()
    }

    #[test]
    fn a_dump_restores_the_log_and_the_application_position() {
        let (bytes, through) = dump();
        assert_eq!(
            through,
            OpNumber(3),
            "a leader's application holds its own ops"
        );
        let node = Node::restore(config(1, 8), &bytes, Duration::ZERO).unwrap();
        let s = node.status();
        assert_eq!(
            (s.log_last, s.applied, s.role),
            (OpNumber(3), OpNumber(3), crate::node::Role::Electing)
        );
        assert!(node.view() >= 1, "it asks for the next view");
    }

    #[test]
    fn damaged_short_or_foreign_dumps_are_refused() {
        let (bytes, _) = dump();
        for end in [0, 7, HEAD_BYTES, bytes.len() - 1] {
            assert_eq!(
                parse(&config(1, 8), &bytes[..end]).err(),
                Some(DumpError::Malformed)
            );
        }
        let mut flipped = bytes.to_vec();
        flipped[HEAD_BYTES + 3] ^= 1;
        assert_eq!(
            parse(&config(1, 8), &flipped).err(),
            Some(DumpError::Malformed)
        );
        assert_eq!(
            parse(&config(2, 8), &bytes).err(),
            Some(DumpError::Foreign),
            "another node's"
        );
        assert_eq!(
            parse(&config(1, 9), &bytes).err(),
            Some(DumpError::Foreign),
            "another cluster's"
        );
    }

    #[test]
    fn a_dump_whose_ops_do_not_reach_its_accepted_op_is_refused() {
        let (bytes, _) = dump();
        let mut body = bytes[..bytes.len() - 32].to_vec();
        // Claim one op fewer than dumped: the history no longer chains.
        let count = HEAD_BYTES - 4;
        body[count..count + 4].copy_from_slice(&2u32.to_be_bytes());
        let mut cut = body.clone();
        let last_op = cut.len() - (8 + 2 + 4 + 5);
        cut.truncate(last_op);
        let check = hash(CHECK_CONTEXT, &cut);
        cut.extend_from_slice(check.as_bytes());
        assert_eq!(parse(&config(1, 8), &cut).err(), Some(DumpError::History));
    }

    #[test]
    fn a_recovering_node_has_nothing_to_dump() {
        let node = Node::recover(config(2, 8), Duration::ZERO).unwrap();
        assert!(node.export().is_none());
    }
}

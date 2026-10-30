//! Forced starts: when no quorum can prove what was committed, one node
//! begins a new configuration epoch alone, from the state it has. The new
//! configuration lists the same voters with that node first, so it leads
//! view zero, and its digest names the epoch and that node. Nodes without
//! state that hear of a newer epoch join it; nodes with state keep theirs
//! and say so in their status.

use std::time::Duration;

use super::{Event, Node, NodeConfig, NodeError, dump};
use crate::wire::WireError;
use crate::{Configuration, Digest, NodeId, integrity};

const FORCED: &str = "zzq-cluster forced configuration";

/// The configurations a node has known: the one it was given, and the
/// newest epoch it heard of and did not join.
#[derive(Debug, Clone, Copy)]
pub(super) struct Epochs {
    pub(super) base: Configuration,
    pub(super) newer: Option<u64>,
}

/// `base` forced to `epoch` by `first`.
pub(super) fn forced(base: &Configuration, epoch: u64, first: NodeId) -> Option<Configuration> {
    let mut rotated = *base.voters();
    let at = rotated.iter().position(|&v| v == first)?;
    rotated.rotate_left(at);
    let scope = base.scope();
    let mut named = Vec::with_capacity(32 + 8 + 16);
    named.extend_from_slice(scope.configuration_digest.as_bytes());
    named.extend_from_slice(&epoch.to_be_bytes());
    named.extend_from_slice(first.as_bytes());
    let digest = integrity::hash(FORCED, &named);
    Configuration::new(scope.group_id, epoch, digest, rotated).ok()
}

/// The configuration `epoch` and `digest` name: `base` itself, or `base`
/// forced to that epoch by one of its voters.
pub(super) fn named(base: &Configuration, epoch: u64, digest: Digest) -> Option<Configuration> {
    if base.scope().configuration_epoch == epoch && base.scope().configuration_digest == digest {
        return Some(*base);
    }
    base.voters()
        .iter()
        .filter_map(|&first| forced(base, epoch, first))
        .find(|c| c.scope().configuration_digest == digest)
}

impl Node {
    /// This node, forced to lead a new epoch of its configuration alone.
    /// `dump`, an exit dump of an earlier run, names the epoch it follows.
    /// The node leads once another node joins, which only nodes without
    /// state do; its application then proposes the whole state it holds,
    /// as the new epoch's history starts empty. Committed ops the other
    /// nodes held and it did not are lost.
    ///
    /// # Errors
    ///
    /// [`NodeError::Dump`] for a dump of another configuration, and as
    /// [`Node::bootstrap`].
    pub fn force(
        config: NodeConfig,
        dump: Option<&[u8]>,
        now: Duration,
    ) -> Result<Node, NodeError> {
        let base = config.configuration;
        let epoch = match dump {
            Some(d) => dump::epoch_of(&config, d)?,
            None => base.scope().configuration_epoch,
        };
        let next = epoch.checked_add(1).ok_or(NodeError::Image)?;
        let configuration = forced(&base, next, config.local).ok_or(NodeError::UnknownNode)?;
        let mut node = Node::bootstrap(
            NodeConfig {
                configuration,
                ..config
            },
            now,
        )?;
        node.epochs.base = base;
        Ok(node)
    }

    /// The configuration epoch the node runs.
    pub const fn epoch(&self) -> u64 {
        self.config.configuration.scope().configuration_epoch
    }

    /// A HELLO of another configuration of this one. A node without state
    /// joins a newer epoch: empty, while the epoch's history is, else by
    /// recovering. A node with state keeps it and notes the epoch.
    pub(super) fn other_epoch(
        &mut self,
        hello: super::link::Hello,
        now: Duration,
    ) -> Result<(), NodeError> {
        let base = self.epochs.base;
        let config = named(&base, hello.epoch, hello.digest)
            .filter(|c| c.scope().configuration_epoch > self.epoch())
            .ok_or(WireError::Peer)?;
        if self.recovering.is_none() {
            let newer = self
                .epochs
                .newer
                .map_or(hello.epoch, |e| e.max(hello.epoch));
            self.epochs.newer = Some(newer);
            return Err(NodeError::NewerEpoch);
        }
        let config = NodeConfig {
            configuration: config,
            ..self.config
        };
        let mut node = if hello.fresh {
            Node::bootstrap(config, now)?
        } else {
            Node::recover(config, now)?
        };
        node.epochs.base = base;
        // An image it took for the old epoch is of no use.
        node.events.push_back(Event::Reset);
        for i in super::link::Side::BOTH {
            node.say_hello(i)?;
        }
        *self = node;
        self.on_hello(hello, now)
    }
}

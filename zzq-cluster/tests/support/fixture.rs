//! The configuration and identities most cluster tests share.

#![allow(dead_code, reason = "each test file uses a part")]

use zzq_cluster::{Configuration, Digest, GroupId, LinkSessionId, NodeId};

/// Voter `index` of the three.
pub(crate) const fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index + 1; 16])
}

/// Group 7, epoch 1, with voters 0, 1 and 2.
pub(crate) fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [node(0), node(1), node(2)],
    )
    .unwrap()
}

/// The link session of a bound pair.
pub(crate) const fn session() -> LinkSessionId {
    LinkSessionId::from_bytes([9; 16])
}

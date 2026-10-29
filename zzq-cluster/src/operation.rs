//! Replicated operations: their identity, kind and digest chain.

use crate::GroupId;
use crate::integrity::{IntegrityHasher as Hasher, hash};

const BODY_HASH_CONTEXT: &str = "zzq cluster operation body v1";
const OPERATION_HASH_CONTEXT: &str = "zzq cluster operation v1";

/// A fixed-width digest: XXH3-128 followed by 16 zero bytes, see
/// [`crate::integrity`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    /// Digest of the empty history.
    pub const ZERO: Self = Self([0; 32]);

    /// The digest with these bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Its bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// What an operation's body holds. The body itself is opaque to this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum OperationKind {
    /// No state change: marks a position in the history.
    Barrier = 1,
    /// State changes for the application to apply.
    Changes = 2,
}

impl TryFrom<u16> for OperationKind {
    type Error = u16;

    fn try_from(value: u16) -> Result<Self, u16> {
        match value {
            1 => Ok(Self::Barrier),
            2 => Ok(Self::Changes),
            _ => Err(value),
        }
    }
}

/// One operation ready for the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalOperation<'a> {
    /// Replication group identity.
    pub group_id: GroupId,
    /// Membership/configuration fence at admission.
    pub configuration_epoch: u64,
    /// Original admission view, preserved through replication and repair.
    pub original_view: u64,
    /// Group-local operation number.
    pub op_number: u64,
    /// Chain digest immediately before this operation.
    pub previous_digest: Digest,
    /// What the body holds.
    pub kind: OperationKind,
    /// Exact body bytes covered by the digest.
    pub body: &'a [u8],
}

/// Operation identity without its body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationHeader {
    /// Replication group identity.
    pub group_id: GroupId,
    /// Membership/configuration fence at admission.
    pub configuration_epoch: u64,
    /// Original admission view, preserved through replication and repair.
    pub original_view: u64,
    /// Group-local operation number.
    pub op_number: u64,
    /// Chain digest immediately before this operation.
    pub previous_digest: Digest,
    /// What the body holds.
    pub kind: OperationKind,
}

impl CanonicalOperation<'_> {
    /// Its identity without the body.
    pub const fn header(&self) -> OperationHeader {
        OperationHeader {
            group_id: self.group_id,
            configuration_epoch: self.configuration_epoch,
            original_view: self.original_view,
            op_number: self.op_number,
            previous_digest: self.previous_digest,
            kind: self.kind,
        }
    }
}

impl OperationHeader {
    /// Chain digest after this operation, given its body's digest.
    pub fn digest(&self, body_digest: Digest) -> Digest {
        let mut hasher = Hasher::new(OPERATION_HASH_CONTEXT);
        hasher.update(self.group_id.as_bytes());
        hasher.update(&self.configuration_epoch.to_be_bytes());
        hasher.update(&self.original_view.to_be_bytes());
        hasher.update(&self.op_number.to_be_bytes());
        hasher.update(&(self.kind as u16).to_be_bytes());
        hasher.update(self.previous_digest.as_bytes());
        hasher.update(body_digest.as_bytes());
        hasher.finish()
    }
}

/// Digest of an operation body.
pub fn canonical_body_digest(body: &[u8]) -> Digest {
    hash(BODY_HASH_CONTEXT, body)
}

/// Chain digest after `operation`.
pub fn logical_operation_digest(operation: &CanonicalOperation<'_>) -> Digest {
    logical_operation_digest_with_body_digest(operation, canonical_body_digest(operation.body))
}

/// Chain digest after `operation`, given its already checked body digest.
pub fn logical_operation_digest_with_body_digest(
    operation: &CanonicalOperation<'_>,
    body_digest: Digest,
) -> Digest {
    operation.header().digest(body_digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(body: &[u8]) -> CanonicalOperation<'_> {
        CanonicalOperation {
            group_id: GroupId::from_bytes([1; 16]),
            configuration_epoch: 1,
            original_view: 0,
            op_number: 1,
            previous_digest: Digest::ZERO,
            kind: OperationKind::Changes,
            body,
        }
    }

    #[test]
    fn the_digest_covers_body_and_identity() {
        let a = logical_operation_digest(&operation(b"a"));
        assert_ne!(a, logical_operation_digest(&operation(b"b")));
        let mut later = operation(b"a");
        later.op_number = 2;
        assert_ne!(a, logical_operation_digest(&later));
        assert_eq!(&a.as_bytes()[16..], &[0; 16]);
    }

    #[test]
    fn kinds_round_trip_and_unknown_ones_fail() {
        for kind in [OperationKind::Barrier, OperationKind::Changes] {
            assert_eq!(OperationKind::try_from(kind as u16), Ok(kind));
        }
        assert_eq!(OperationKind::try_from(9), Err(9));
    }
}

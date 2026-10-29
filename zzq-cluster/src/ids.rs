//! 128-bit identities of nodes, groups, requests and link sessions.

use std::fmt;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Distinguishes identities made in the same nanosecond.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 16 bytes no other call returns: the time, a counter and a per-process
/// random seed. Fresh, not secret.
fn fresh() -> [u8; 16] {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    // Relaxed: only uniqueness matters, not order.
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let seed = RandomState::new().hash_one((nanos, n));
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&(nanos as u64).to_be_bytes());
    bytes[8..].copy_from_slice(&(seed ^ n).to_be_bytes());
    if bytes == [0; 16] {
        bytes[15] = 1;
    }
    bytes
}

macro_rules! id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 16]);

        impl $name {
            /// A fresh, nonzero identity.
            pub fn new() -> Self {
                Self(fresh())
            }

            /// The identity with these bytes.
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            /// Its bytes.
            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for b in self.0 {
                    write!(f, "{b:02x}")?;
                }
                Ok(())
            }
        }
    };
}

id!(
    /// A node of the cluster.
    NodeId
);
id!(
    /// A replication group: the cluster's state.
    GroupId
);
id!(
    /// A request, for correlating replies and as a recovery nonce.
    RequestId
);
id!(
    /// One established link between two nodes.
    LinkSessionId
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_identities_differ_and_are_nonzero() {
        let a = NodeId::new();
        let b = NodeId::new();
        assert_ne!(a, b);
        assert_ne!(a.as_bytes(), &[0; 16]);
    }

    #[test]
    fn identities_print_as_hex() {
        let id = GroupId::from_bytes([0xab; 16]);
        assert_eq!(id.to_string(), "ab".repeat(16));
        assert_eq!(format!("{id:?}"), format!("GroupId({})", "ab".repeat(16)));
    }
}

//! Realtime synchronized Rust state for Firebase Realtime Database.
//!
//! `rtdb-sync` is intentionally responsible for synchronization semantics,
//! not Firebase transport or Serde conversion. The 0.1.0 implementation will
//! build on `rtdb-typed` once its typed realtime event contract is finalized.

/// Marker used while the 0.1.0 synchronization API is being designed.
///
/// No stability guarantee is attached to this pre-release scaffold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStatus {
    /// No synchronization task has started.
    Idle,
    /// Initial state is being hydrated.
    Hydrating,
    /// A realtime stream is active.
    Connected,
    /// Synchronization has stopped.
    Stopped,
}

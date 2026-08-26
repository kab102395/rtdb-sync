# rtdb-sync

Realtime synchronized Rust state for Firebase Realtime Database, built on `rtdb-typed` and `rtdb-rs`.

`rtdb-sync` is the state-management layer in the RTDB Rust ecosystem. It does not reimplement Firebase REST transport, authentication, query construction, or typed Serde conversion.

## Responsibility boundary

```text
application
    |
rtdb-sync        hydration, local state, event application, subscriptions, reconnect policy
    |
rtdb-typed       typed Serde CRUD/query/event conversion
    |
rtdb-rs          Firebase REST + SSE transport
    |
Firebase RTDB
```

## Implemented foundation and synchronization

The core supports Firebase -> Rust synchronization plus delegated local writes:

- initial state hydration
- typed local state snapshot
- application of Firebase `Put` and partial `Patch` events
- deletion/null handling
- subscriber/watch API
- connection status
- graceful shutdown
- deterministic localhost tests
- Firebase Realtime Database emulator integration
- concurrency and fan-out stress testing
- confirmed or optimistic local PUT/PATCH writes
- bounded write queues, acknowledgement tracking, rollback, echo suppression,
  and explicit conflict policies

See `docs/ROADMAP.md` and `docs/ENVIRONMENT_AND_STRESS_TESTING.md`.

## Durable offline mode

Durable mode is opt-in. Supply a `FilePersistence` (or your own
`PersistenceBackend`), a stable `persistence_key`, and
`OfflinePolicy::QueueWhileOffline` or `QueueWithLimit`. Restored state is
reported as `RestoredStale`; after hydration and replay the handle reports
`Connected`.

```rust,no_run
use std::{path::PathBuf, sync::Arc};
use rtdb_sync::{Config, FilePersistence, OfflinePolicy};

let persistence = Arc::new(FilePersistence::new(PathBuf::from("./state"))?);
let config = Config {
    persistence: Some(persistence),
    persistence_key: Some("account-42/profile".into()),
    offline_policy: OfflinePolicy::QueueWhileOffline,
    ..Config::default()
};
```

Snapshots are atomic/fsynced and the journal is append-oriented and
versioned. The journal does not persist Firebase credentials or OAuth tokens.
The file backend stores application data as plaintext; sensitive or regulated
data requires an encrypted `PersistenceBackend`. Replay is at-least-once
across the crash window between remote success and durable acknowledgement, so
application writes should be replacement/idempotent operations where possible.

## Status

The synchronization core is implemented: hydration, typed snapshots,
PUT/PATCH/null event application, watch notifications, cancellation, reconnect
backoff, confirmed or optimistic writes, conflict handling, and a Firebase
REST/SSE backend are available. The API remains pre-1.0 and may change.

Run the deterministic and lint gates with:

```text
cargo fmt --all -- --check
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo package --allow-dirty
cargo publish --dry-run --allow-dirty
./scripts/test-emulator.sh
```

The emulator command is restricted to `demo-*` projects and runs the CRUD/SSE
integration test plus the local concurrency profiles. These are correctness
stress tests, not production capacity benchmarks.

## License

MIT

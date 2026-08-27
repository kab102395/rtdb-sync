# rtdb-sync

Realtime synchronized Rust state for Firebase Realtime Database, with opt-in durable offline recovery.

`rtdb-sync` is the state-management layer of the RTDB Rust ecosystem. It owns hydration, maintained local state, local writes, reconnect behavior, conflict handling, durability, offline queueing, process-restart recovery, replay, and reconciliation while delegating typed conversion to `rtdb-typed` and Firebase transport to `rtdb-rs`.

## RTDB Rust ecosystem

```text
application
   |
   +-- rtdb-sync
   |     synchronized state, durable snapshots, offline journal,
   |     reconnect/replay, reconciliation, conflict policy
   |
   +-- rtdb-typed
   |     Serde models, typed CRUD, collections, queries, realtime events
   |
   +-- rtdb-admin
   |     service-account loading, OAuth exchange, token lifecycle
   |
   `-- rtdb-rs
         Firebase REST + query + SSE transport
                  |
                  v
          Firebase Realtime Database
```

| Crate | Responsibility |
| --- | --- |
| [`rtdb-rs`](https://github.com/kab102395/rtdb-rs) | Raw Firebase REST/query/SSE transport |
| [`rtdb-typed`](https://github.com/kab102395/rtdb-typed) | Typed Serde CRUD, collections, queries, patches, and realtime events |
| [`rtdb-admin`](https://github.com/kab102395/rtdb-admin) | Service-account credentials and OAuth token lifecycle |
| [`rtdb-sync`](https://github.com/kab102395/rtdb-sync) | Synchronized state, local writes, durability, offline replay, and reconciliation |

`rtdb-sync` does not reimplement Firebase REST/SSE transport and does not own service-account keys or OAuth token lifecycle.

## Current release line

The current package is `0.4.0`.

The synchronization core now covers:

- initial Firebase hydration
- typed local snapshots
- Firebase `Put`, partial `Patch`, and deletion/null application
- subscriber/watch notifications
- connection/synchronization status
- reconnect with bounded backoff
- graceful shutdown and cancellation
- confirmed and optimistic local PUT/PATCH writes
- bounded write queues
- acknowledgement tracking
- rollback behavior
- remote echo suppression
- explicit conflict policies
- durable snapshots
- append-oriented mutation journaling
- offline local-write queueing
- process-restart state restoration
- replay after reconnect
- durable acknowledgement and journal compaction
- typed and raw Firebase transport adapters
- official Firebase Realtime Database Emulator integration
- multi-path, fan-out, restart, durability, and long-duration stress validation

## Responsibility boundary

```text
                         TypedSyncHandle<T>
                               |
            +------------------+------------------+
            |                  |                  |
        local state       mutation queue      sync status
            |                  |                  |
            +------------------+------------------+
                               |
                         rtdb-sync engine
                               |
              +----------------+----------------+
              |                                 |
       PersistenceBackend                 RemoteBackend
              |                                 |
      snapshot + journal                  rtdb-typed
                                                |
                                             rtdb-rs
                                                |
                                         Firebase RTDB
```

The application chooses synchronization and conflict policy. `rtdb-sync` is not presented as a distributed transaction protocol or CRDT implementation.

## Durable offline mode

Durable mode is opt-in. Supply a persistence backend, a stable persistence key, and an offline policy.

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

Supported offline policy includes rejecting writes while offline, queueing while offline, and bounded queueing when a maximum pending count is configured.

A durable handle can restore the last committed local snapshot before Firebase is reachable. Restored state is explicitly stale until remote hydration/reconciliation completes; the status API allows the application to distinguish restored/offline state from a currently synchronized connection.

## Persistence and crash recovery

The built-in file persistence layer uses atomic/fsynced snapshots and a versioned append-oriented journal.

The journal is designed so supported pending local mutations survive process restart. Successful replay acknowledgement is persisted so acknowledged work can be compacted rather than intentionally replayed forever.

There is an unavoidable crash window where a write may reach Firebase but the process can die before recording the local durable acknowledgement. `rtdb-sync` therefore provides at-least-once replay semantics across that window rather than claiming exactly-once delivery. Replacement/idempotent writes, stable keys, or application-level idempotency remain appropriate where duplicate delivery matters.

The persistence API does not store service-account private keys or OAuth tokens. The built-in file backend stores application data as plaintext; applications handling sensitive or regulated data should provide an encrypted `PersistenceBackend`.

## Conflict and reconciliation behavior

The synchronization engine exposes explicit conflict policy instead of silently discarding concurrent state.

The 0.4 line supports the baseline policies needed for local-vs-remote reconciliation while writes are pending. Production conflict semantics remain an application decision rather than a claim of universal distributed consistency.

## Testing and validation

The repo contains deterministic unit/mock tests plus official Firebase Realtime Database Emulator validation for:

- hydration and realtime event application
- null/deletion handling
- typed conversion failures
- multi-path namespace isolation
- reconnect/backoff behavior
- confirmed and optimistic writes
- write failures and rollback
- concurrent local and remote writers
- fan-in and subscriber fan-out
- stream restart/recovery
- durable snapshot restoration
- offline pending mutation recovery
- process death between queueing and replay
- replay while remote writers remain active
- acknowledgement persistence and zero-pending completion

The full ecosystem stress harness also exercises all four crates together: `rtdb-rs`, `rtdb-typed`, `rtdb-admin`, and `rtdb-sync`.

It includes actual local `rtdb-sync` writes concurrently with raw, typed, and admin-driven remote writes; two active subscribers per synchronized path; short controlled token lifetimes and active client replacement; deterministic expected-state comparison against both emulator reads and sync snapshots; and durable outage/process-restart/replay flows without a final repair write masking earlier errors.

## Measured local stress envelope

The current recorded local evidence includes:

- 100 synchronized paths, two subscribers per path, 10,000 mixed mutations, admin client replacement, and final convergence
- 250 paths × 1,800 generations passed in 32.49 seconds with 154,752 KiB maximum Rust test RSS
- 100 paths × 100,000 generations passed in 14:00.90 with 2,871,168 KiB maximum Rust RSS and final convergence
- 250 paths × 100,000 generations passed in 30:44.69 with 7,050,432 KiB maximum Rust RSS and final convergence
- durable offline seed → emulator-down queue → deliberate process exit → emulator restart → replay with 100 active remote admin writes, ending with replay success and zero pending mutations
- repeated connection-boundary runs that pass the 100- and 150-path tiers consistently, show mixed pass/capacity behavior around 200 paths, and reach a setup capacity wall at the 500-path heavy tier

These are local correctness, endurance, and capacity-boundary measurements on a Firebase Emulator and development host. They are not universal Firebase production-service capacity guarantees.

The stress harness classifies `CAPACITY_LIMIT`, `CORRECTNESS_FAILURE`, and harness/environment failures separately so saturation is not confused with a correctness pass.

See:

- `docs/ROADMAP.md`
- `docs/ROADMAP_0.4.0_OFFLINE_SYNC.md`
- `docs/ENVIRONMENT_AND_STRESS_TESTING.md`
- `docs/ECOSYSTEM_PRODUCTION_STRESS_PLAN.md`

## Test commands

Source and deterministic gates:

```text
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Firebase Emulator and durable acceptance:

```text
./scripts/test-emulator.sh
./scripts/test-emulator-offline.sh
./scripts/test-ecosystem-durable.sh
```

Production-style local stress profiles:

```text
./scripts/test-ecosystem-stress.sh standard
./scripts/test-ecosystem-heavy.sh
./scripts/test-ecosystem-soak.sh
./scripts/test-ecosystem-breakpoint.sh
```

The emulator runners are restricted to `demo-*` project IDs and refuse unsafe local port conflicts.

## CI and coordinated release gate

Clean-checkout CI currently validates formatting, Clippy with warnings denied, and all-target/all-feature tests using pinned development revisions for the companion crates where publication order requires it.

The final crates.io release graph is intentionally a separate gate. `rtdb-sync 0.4.0` expects the coordinated `rtdb-rs 0.3.2` line; package and publish dry-runs against the final registry dependency graph are run after the matching upstream release exists on crates.io.

The release sequence is therefore:

```text
rtdb-rs 0.3.2
        |
        +--> rtdb-typed
        +--> rtdb-admin
        `--> rtdb-sync 0.4.0
```

Before publication, the companion crates are revalidated against the actual crates.io dependencies rather than relying only on git/path development pins.

## Relationship to rtdb-admin

Server-side applications can use [`rtdb-admin`](https://github.com/kab102395/rtdb-admin) to own service-account loading, OAuth token exchange, expiry, refresh, and authenticated-client replacement. Those credentials and tokens remain outside the persistence layer.

## Relationship to rtdb-typed and rtdb-rs

[`rtdb-typed`](https://github.com/kab102395/rtdb-typed) owns typed Serde model/query/event semantics, including explicit partial `TypedPatch` behavior.

[`rtdb-rs`](https://github.com/kab102395/rtdb-rs) owns Firebase REST, query construction, namespaces, and SSE transport.

`rtdb-sync` composes those capabilities into maintained application state rather than replacing them.

## Scope

`rtdb-sync` is a Firebase Realtime Database synchronization layer. It is not a Firestore client, ORM, complete offline-first database, CRDT framework, or exactly-once distributed transaction system.

The API remains pre-1.0 and may change as real-world adoption exposes additional synchronization and persistence requirements.

## License

MIT

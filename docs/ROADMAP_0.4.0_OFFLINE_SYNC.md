# rtdb-sync 0.4.0 roadmap — durable offline synchronization foundation

## Current implementation status

The current worktree implements the replaceable memory/file persistence
boundary, atomic versioned snapshots, append-only mutation journaling,
acknowledgement and compaction, explicit offline queue policies, stale restore,
replay after reconnect, corruption detection, namespace isolation, and the
three-process live-emulator acceptance flow in
`scripts/test-emulator-offline.sh`. The file backend is plaintext by design;
callers needing encryption must provide an encrypted persistence backend.

## Purpose

`rtdb-sync` 0.4.0 is the first offline-capable release milestone for the RTDB Rust ecosystem.

The goal is not to claim a complete distributed database, CRDT system, or universal offline-first framework. The goal is to establish the complete base structure required for a Rust application to continue operating through process restarts and temporary network loss while preserving deterministic synchronization with Firebase Realtime Database.

This milestone should be implemented before the coordinated public ecosystem release if the work can be completed without destabilizing the existing 0.1–0.3 synchronization semantics.

The intended user outcome is simple:

> A developer should be able to create an `rtdb-sync` handle, opt into durable local state, lose network connectivity or restart the process, continue from the last durable state, queue supported local mutations, reconnect, replay them safely, and observe when the local state is current again.

## Ecosystem boundary

The existing ownership model remains unchanged:

```text
application
    |
rtdb-sync        synchronized state, persistence, journal, replay, reconciliation
    |
rtdb-typed       typed models, typed queries, typed realtime event semantics
    |
rtdb-rs          Firebase RTDB REST + SSE transport
    |
Firebase RTDB
```

`rtdb-admin` remains a sibling integration layer for server-side credential and token lifecycle management. `rtdb-sync` may accept replacement/authenticated clients or adapters, but it must not absorb service-account/JWT/OAuth lifecycle responsibilities.

Rules:

- transport failures or missing RTDB primitives belong in `rtdb-rs`
- typed serialization, collection, patch, and realtime event semantics belong in `rtdb-typed`
- durable synchronized-state semantics belong in `rtdb-sync`
- service-account credential lifecycle belongs in `rtdb-admin`
- no layer should duplicate another layer merely to make 0.4.0 easier to implement

## 0.4.0 release definition

0.4.0 is considered complete when a user can opt into a durable synchronization mode with these properties:

1. the most recently committed local synchronized snapshot survives process restart
2. pending supported local mutations survive process restart
3. startup can restore local state before Firebase is reachable
4. the application can distinguish restored/stale state from fully synchronized/current state
5. local mutations can be accepted while offline when policy allows
6. queued mutations replay after connectivity returns
7. successful replay is acknowledged durably so acknowledged mutations are not intentionally replayed forever
8. remote Firebase events are reconciled with restored/pending local state without silently discarding conflicts
9. cancellation and shutdown flush required durable state according to the documented durability policy
10. crash/failure injection tests prove that journal and snapshot recovery do not silently corrupt state

## Architectural target

```text
                           application
                               |
                         SyncHandle<T>
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

The durable layer must be explicit and replaceable. 0.4.0 should ship with one production-usable reference backend or a clearly documented minimal backend if adding a full embedded database would create unacceptable release risk.

## Phase 1 — persistence abstraction

Create an internal/publicly usable persistence boundary sufficient to store synchronized state without coupling the sync engine to one storage implementation.

Required concepts:

- durable snapshot record
- pending mutation journal
- synchronization metadata
- atomic or crash-safe commit boundary where supported
- schema/version marker for persisted state
- namespace/path identity so unrelated sync handles cannot overwrite one another
- optional application identity/profile partitioning if needed for multi-user local state

The abstraction should be async-friendly but must not require every backend to perform actual asynchronous disk I/O internally.

Expected conceptual operations:

```text
load_snapshot(sync_key)
store_snapshot(sync_key, snapshot)
load_pending_mutations(sync_key)
append_mutation(sync_key, mutation)
mark_mutation_acknowledged(sync_key, mutation_id)
remove_or_compact_acknowledged(sync_key)
store_sync_metadata(sync_key, metadata)
clear(sync_key)
```

Exact API names are intentionally not fixed by this roadmap. The implementation should optimize for a small, stable public surface rather than exposing storage internals.

### Persistence backend requirements

The first backend must document:

- whether writes are atomic
- crash behavior
- fsync/durability expectations
- corruption behavior
- maximum practical snapshot/journal size assumptions
- whether concurrent processes may open the same store
- migration/versioning policy

A memory-only backend should remain available for deterministic tests and users who do not need persistence.

## Phase 2 — durable snapshots and crash recovery

Add persisted synchronized snapshots before offline mutation replay.

Required behavior:

- after a successful hydration or accepted remote state transition, the durable snapshot can be updated
- startup can restore the most recent valid snapshot without contacting Firebase first
- restored state is explicitly marked as restored/stale until remote synchronization confirms freshness
- a corrupt or incompatible snapshot produces a visible error or documented recovery policy; it must never be silently interpreted as valid application state
- state serialization failures remain visible
- snapshot writes must not block the synchronization state machine indefinitely
- snapshot persistence failure must be surfaced through status/metrics/error policy

Recommended state vocabulary:

```text
Idle
Restoring
RestoredStale
Hydrating
ConnectedCurrent
Offline
Replaying
Conflict
Failed
Stopped
```

The exact enum may differ, but callers must be able to tell the difference between usable local state and confirmed-current remote state.

### Snapshot release gate

A process can:

1. synchronize a typed RTDB path
2. persist it
3. terminate
4. restart with Firebase unavailable
5. restore the same typed state locally
6. expose that state as restored/stale rather than current
7. reconnect later and rehydrate/reconcile successfully

## Phase 3 — durable mutation journal

Introduce a durable write-ahead mutation journal for supported local writes.

Each queued mutation should have enough information to support deterministic recovery. The journal will likely need concepts equivalent to:

- stable mutation ID
- target RTDB path
- mutation kind (`put`, `patch`, `delete`, and any supported atomic update form)
- serialized typed/raw payload as appropriate
- creation/order sequence
- local generation/revision context
- retry count or last attempt metadata where useful
- acknowledgement state

The journal should be append-oriented. Avoid rewriting the entire queue on every mutation unless benchmarks prove that simplicity is worth the cost.

### Ordering rule

0.4.0 must define an explicit ordering guarantee.

Recommended baseline:

> Mutations created by one sync handle are replayed in durable journal order unless a documented coalescing rule proves equivalence.

Do not introduce aggressive mutation coalescing in the first implementation unless its correctness is trivial and heavily tested.

### Journal release gate

A mutation accepted locally and durably journaled must survive process termination before remote acknowledgement.

## Phase 4 — offline local mutation policy

Add an explicit offline write policy rather than implicitly accepting every mutation.

Suggested policy model:

```text
RejectWhileOffline
QueueWhileOffline
QueueWithLimit { max_pending }
```

The implementation may use different names, but the behavior must be opt-in and deterministic.

When queueing is enabled:

- the local synchronized state may update optimistically if configured
- the durable journal must be written before the mutation is treated as crash-recoverable
- a journal write failure must prevent the application from believing the mutation is safely durable
- queue bounds must produce a visible error rather than silently dropping mutations
- pending mutation count should be observable

## Phase 5 — reconnect replay

When connectivity returns, replay durable pending mutations through the existing typed/transport layers.

Required behavior:

- replay begins only when the remote backend is usable
- replay order is deterministic
- cancellation interrupts replay cleanly
- retry/backoff rules integrate with the existing 0.2 reconnect state machine rather than creating a second independent retry engine
- permanent write failures become visible terminal/policy events
- successful acknowledgements are persisted
- the queue can recover if the process terminates during replay

### Critical crash window

The hardest baseline case is:

```text
journal says pending
    |
write reaches Firebase successfully
    |
process dies before local acknowledgement is persisted
```

0.4.0 must document the behavior of this ambiguity.

The first implementation does not need magical exactly-once distributed semantics. It does need an explicit policy and testable guarantees.

Acceptable initial strategies include:

- application-level idempotency guidance
- deterministic replacement writes where replay is naturally idempotent
- conditional/revision-aware writes if supported by the upstream transport
- duplicate-tolerant semantics for push-like operations only when a stable key is generated before journaling

Never describe the system as exactly-once unless the implementation can prove that property across process and network failure.

## Phase 6 — acknowledgement and compaction

Acknowledged mutations should not grow the journal forever.

Required features:

- durable acknowledgement marker or atomic removal
- safe compaction
- compaction that cannot delete still-pending mutations after a crash
- bounded disk-growth strategy
- observability for pending and retained journal entries

A simple threshold-based compaction policy is sufficient for 0.4.0.

## Phase 7 — reconciliation and conflict foundation

0.4.0 should establish the conflict framework even if advanced automatic merge strategies are deferred.

Required conflict inputs:

- restored snapshot
- pending local mutations
- newly hydrated remote state
- incoming remote realtime events
- local generation/revision metadata

Required baseline policies:

```text
ServerWins
LocalWins
RejectAndSurface
```

A custom callback/policy trait may be introduced if the API can remain clear and deterministic.

Do not silently default every ambiguous case to last-writer-wins without documenting it.

### Conflict event

Applications should be able to observe conflict details sufficient to make a decision, including:

- affected path
- local/pending representation
- remote representation when available
- mutation ID(s) involved
- chosen/default resolution policy

Sensitive values should not be automatically exposed through `Debug` if applications may synchronize secrets.

## Phase 8 — remote echo and pending-write reconciliation

The existing self-write/echo semantics must integrate with the durable journal.

The engine must be able to distinguish, as far as the available RTDB semantics allow:

- a remote event confirming a pending local write
- a conflicting remote write
- an unrelated remote write
- a replayed local write after restart

The solution may use local generations, mutation IDs tracked outside Firebase, stable keys, payload comparison, or upstream conditional-write capabilities. The chosen semantics must be documented and stress tested.

## Phase 9 — observability

Offline synchronization without visibility will be difficult to operate.

0.4.0 should expose enough structured state/metrics for applications to answer:

- are we online?
- is local state current?
- was state restored from disk?
- how old is the durable snapshot?
- how many writes are pending?
- what is the oldest pending mutation age?
- are we currently replaying?
- how many replay attempts have occurred?
- did persistence fail?
- did reconciliation produce a conflict?
- when was the last confirmed remote event?

Recommended metrics/events:

```text
connection_state
snapshot_state
snapshot_age
pending_mutations
oldest_pending_age
replay_attempts
replay_successes
replay_failures
conflict_count
persistence_failures
last_remote_event_at
last_successful_sync_at
```

No metrics backend should be mandatory; counters/state should be available for integration into user-selected telemetry.

## Phase 10 — multi-path and namespace safety

Durable offline state must work for multiple simultaneous synchronization handles.

Required tests:

- multiple independent RTDB paths
- overlapping parent/child paths if supported
- separate local persistence namespaces
- concurrent remote events
- concurrent local writes
- independent queue recovery
- shutdown of one handle without corrupting another

Avoid introducing a global singleton persistence manager unless the design clearly requires it.

## Phase 11 — storage migration/versioning

Persisted state is a compatibility surface once released.

Before 0.4.0 ships, define:

- storage format version
- behavior when the crate sees a newer unsupported format
- migration path for compatible older formats
- application-controlled reset/clear operation
- corruption detection where practical

Do not rely solely on Rust type layout or unstable binary representations for long-lived durable state.

Serde-based versioned records are preferred for the first release unless benchmarks justify something else.

## Phase 12 — security and data handling

Offline persistence changes the security model because Firebase data may now exist on local disk.

0.4.0 documentation must state clearly:

- persistence may contain application data in plaintext unless the selected backend provides encryption
- `rtdb-sync` does not automatically provide OS keychain or full-disk encryption
- callers handling secrets, health data, credentials, or regulated information must choose an appropriate encrypted-at-rest strategy
- credentials and OAuth tokens must not be persisted by the sync journal
- service-account lifecycle remains owned by `rtdb-admin`

A future encrypted persistence backend can be considered separately. Do not delay the core synchronization model solely to invent encryption unless the selected storage backend makes it straightforward.

## Phase 13 — deterministic failure-injection suite

0.4.0 is not complete without failure testing.

The test suite should be able to inject failures at these boundaries:

### Snapshot failures

- crash before snapshot write
- crash during snapshot replacement
- corrupt snapshot
- incompatible storage version
- disk write failure
- snapshot serialization failure

### Journal failures

- crash before journal append
- crash immediately after journal append
- crash after optimistic local apply but before remote send
- crash after remote success but before durable acknowledgement
- journal corruption
- queue limit exhaustion

### Network/replay failures

- offline at startup
- stream disconnect during replay
- write failure during replay
- repeated reconnect churn
- cancellation during backoff
- cancellation during replay
- remote state changes while offline
- concurrent remote writer during replay

### Recovery assertions

After each recoverable failure:

- no acknowledged mutation should return to pending due solely to compaction bugs
- no pending durable mutation should disappear silently
- local state should either recover deterministically or enter a visible failed/conflict state
- task count should not leak across repeated recovery cycles
- restored state should never be mislabeled as confirmed-current before reconciliation

## Phase 14 — emulator acceptance scenario

Create one end-to-end Firebase Emulator scenario that demonstrates the entire 0.4.0 value proposition.

Suggested acceptance flow:

```text
1. start emulator
2. hydrate typed state
3. persist snapshot
4. stop sync process
5. stop emulator / simulate network loss
6. restart sync process
7. restore state from disk
8. perform supported offline local mutations
9. verify mutations are durable
10. restart process again while still offline
11. verify restored optimistic state + pending journal
12. restart emulator
13. reconnect
14. rehydrate/reconcile
15. replay pending writes
16. verify acknowledgements
17. verify journal compaction
18. verify final local == Firebase state
```

This should be a documented reproducible test, not just an internal unit case.

## Phase 15 — public API ergonomics

The first offline API should require very little setup for the common case.

Conceptually, a developer should be able to provide:

```text
TypedClient / remote backend
sync path
persistence backend
sync policy
```

and receive a handle exposing:

```text
current snapshot
watch/subscription
connection/currentness state
local mutation API
pending mutation status
shutdown
```

Avoid forcing users to manually coordinate a snapshot store, mutation queue, reconnect loop, and SSE task themselves. That orchestration is the value of `rtdb-sync`.

## Phase 16 — ecosystem integration before coordinated publish

Before publishing the four-crate ecosystem, verify 0.4.0 against the intended release graph:

```text
rtdb-rs      0.3.2   REST + SSE transport foundation
rtdb-typed   final   typed CRUD/query/realtime semantics
rtdb-admin   0.1.0   server credential/token lifecycle
rtdb-sync    0.4.0   live + durable offline synchronized state
```

Final dependency requirements:

- remove temporary git/path development dependencies
- depend on the actual published or coordinated crate versions
- no `[patch.crates-io]` sections in publishable manifests unless intentionally required and supported by Cargo packaging
- run all-features builds against the same dependency graph users will receive

Required final gates for `rtdb-sync`:

```text
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo doc --no-deps --all-features
cargo package
cargo publish --dry-run
./scripts/test-emulator.sh
./scripts/test-emulator-recovery.sh
```

If a heavier offline acceptance script is added, it should be included in release validation even if not run on every push.

## Documentation required before release

The README and docs must clearly explain:

- what `rtdb-sync` owns
- how it differs from `rtdb-rs` and `rtdb-typed`
- where `rtdb-admin` fits
- live-only mode vs durable/offline mode
- restored/stale vs current state
- optimistic vs confirmed local writes
- offline queue policy
- replay semantics
- conflict policy
- crash ambiguity / idempotency guidance
- local data-at-rest security considerations
- how to test everything with the Firebase Emulator

A complete example should demonstrate the four-crate ecosystem without requiring users to reverse-engineer integration from unit tests.

## Recommended first storage backend decision

The storage abstraction is mandatory. The exact first durable backend can be decided during implementation.

Selection criteria:

- pure Rust or easily distributable
- crash behavior is understandable
- minimal native dependency burden
- supports atomic/transactional updates if practical
- stable enough for a library dependency
- reasonable Windows/macOS/Linux support
- easy to use from Tauri/desktop/server environments

Candidates may include SQLite-backed storage, `redb`, or another embedded store after dependency/security/maintenance review. Do not hard-code the public sync API around the chosen backend.

## Explicit 0.4.0 non-goals

Do not expand 0.4.0 into all possible synchronization research.

Deferred unless required for correctness:

- generic backend-independent sync engine outside Firebase
- CRDT implementation
- peer-to-peer synchronization
- arbitrary field-level merge language
- transparent cross-device exactly-once delivery guarantees
- distributed transactions
- automatic encryption/key management
- Firestore support
- Firebase Authentication user administration
- Storage/FCM/Remote Config/Admin SDK expansion
- framework-specific state adapters for every Rust UI framework

Those can become future ecosystem milestones after the Firebase RTDB offline model is proven.

## Definition of done

`rtdb-sync 0.4.0` is ready when the following statement is true and demonstrable:

> A new Rust user can compose `rtdb-rs`, `rtdb-typed`, optional `rtdb-admin`, and `rtdb-sync`; synchronize typed Firebase RTDB state; persist that state locally; restart without Firebase; continue from an explicitly stale restored snapshot; queue supported local writes while offline; survive another restart; reconnect; replay and acknowledge pending writes; reconcile remote changes under an explicit conflict policy; and verify the entire workflow using deterministic tests and the Firebase Emulator.

That is the base structure required before describing `rtdb-sync` as durable offline-capable synchronized Rust state.

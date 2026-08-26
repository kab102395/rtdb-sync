# rtdb-sync roadmap

## Design boundary

`rtdb-sync` owns synchronized state semantics. `rtdb-typed` owns model conversion and typed events. `rtdb-rs` owns Firebase REST/SSE transport. Missing transport primitives must be fixed upstream rather than duplicated here.

## 0.1.0 — one-way synchronized state

Status: partial foundation only. The transport boundary and basic local event
application exist, but the typed ecosystem integration and complete release
gate are still being implemented.

Goal: maintain a typed local representation of one RTDB path from Firebase realtime events.

Required scope:

- hydrate initial state before exposing a ready snapshot
- accept typed `Put` events as replacement state
- apply partial `Patch` events without pretending they are full models
- handle deletion/null semantics explicitly
- expose current snapshot without requiring callers to parse JSON
- expose subscriber/watch notifications
- expose basic state: idle, hydrating, connected, stopped, failed/cancelled as API design settles
- support explicit graceful shutdown
- never hide unrecoverable conversion errors
- deterministic mock/local tests
- official RTDB emulator CRUD + SSE integration
- documented cancellation and task ownership semantics

Release gate:

A user can synchronize one typed RTDB path into Rust state, observe changes, stop the task cleanly, and reproduce all integration behavior against the local emulator without a Firebase production project.

## 0.2.0 — resilience and reconnect

Status: incomplete. Basic cancellation-aware retry exists, but the explicit
state machine, jitter, instrumentation, replacement hooks, and outage/recovery
test gates do not yet exist.

Goal: make long-running synchronization viable.

Planned scope:

- explicit connection state machine
- reconnect after transient stream/request failures
- configurable exponential backoff
- bounded jitter
- retry ceilings / terminal failure policy
- rehydrate after reconnect before declaring state current
- preserve subscriber semantics through reconnect
- token/client replacement hooks without owning service-account auth
- cancellation while sleeping/backing off
- metrics/events for reconnect attempts and causes
- deterministic simulated outage tests
- emulator restart/recovery tests
- concurrent multi-path synchronization stress tests

Release gate:

Forced stream drops, temporary emulator outages, and reconnect cycles cannot silently corrupt local state or leak synchronization tasks.

## 0.3.0 — bidirectional synchronization

Status: incomplete. Basic delegated PUT/PATCH and optimistic/confirmed modes
exist, but conflict semantics, echo reconciliation, failure injection, and
concurrent-writer guarantees are not implemented.

Goal: safely support Firebase <-> local Rust state.

Planned scope:

- local mutation API
- typed PUT/PATCH writes delegated through lower layers
- optimistic update policy
- write acknowledgement tracking
- rollback on failed writes where deterministic
- echo suppression / self-write reconciliation
- revision or generation tracking
- explicit conflict policy instead of implicit last-writer behavior
- idempotency guidance for application-level writes
- offline/reconnect write policy
- bounded write queue if adopted
- stress tests with concurrent local and remote writers
- deterministic conflict/replay tests

Release gate:

Two-way state changes remain deterministic under concurrent remote updates, local writes, retries, stream reconnects, and write failures.

## Non-goals through 0.3.x

- reimplementing Firebase REST transport
- service-account/JWT/token lifecycle management
- pretending Firebase is a relational ORM
- silently swallowing malformed patches or model errors
- claiming production capacity numbers from local stress tests

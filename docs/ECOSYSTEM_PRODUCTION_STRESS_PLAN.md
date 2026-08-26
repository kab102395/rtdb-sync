# RTDB Rust ecosystem production-style stress and break-point plan

## Current execution status — 2026-08-26

The executable four-crate harness is present in `tests/ecosystem.rs` and
`scripts/test-ecosystem-stress.sh`. It uses a controlled `rtdb-admin`
`TokenManager` to create and replace an `rtdb-rs` client, sends mixed raw,
`rtdb-typed`, and `rtdb-sync` traffic through the local Firebase RTDB
emulator, keeps two subscribers per synchronized path, and verifies typed
local state against final emulator reads. Each run records crate commits,
host metadata, Rust `/usr/bin/time -v` output, and independent emulator JVM
CPU/RSS samples under `artifacts/ecosystem/`.

Evidence from this workstation:

- Standard profile passed with 100 synchronized paths, two subscribers per
  path, 10,000 mixed mutations, admin client replacement, and final
  convergence in 6 seconds.
- `rtdb-rs` namespace/REST/SSE emulator stress passed; `rtdb-typed` CRUD,
  query, SSE, filtered-child, and fan-out emulator profiles passed; the
  controlled `rtdb-admin` suite passed 13 tests including 100 single-flight
  callers, 64 expiry-boundary callers, 128 outage callers, 100 identities,
  and 1,000 concurrent identity consumers.
- Break-point escalation reached 250 synchronized SSE paths and failed to
  establish all handles within the bounded 60-second setup window. At 500
  paths, the Rust process reached about 395 MiB RSS before the same timeout.
  Captured emulator samples show the JVM reaching about 598 MiB RSS. This is
  a measured local emulator/host envelope, not a universal Firebase capacity
  claim; heavy and soak profiles remain release-blocked pending a capacity
  decision or architecture improvement.

The exact crates.io publication graph is not yet testable: crates.io does not
currently provide `rtdb-rs 0.3.2`, so standalone `rtdb-admin` and package/
publish dry-runs requiring that version fail resolution. Local integrated
testing uses the four checked-out crate revisions recorded in each artifact.

## Purpose

This document defines the coordinated stress, endurance, failure-injection, and break-point validation plan for the full Rust Firebase Realtime Database ecosystem:

```text
application / stress harness
    |
    +-- rtdb-admin      service-account auth, OAuth exchange, token lifecycle
    |
    +-- rtdb-sync       synchronized state, durability, replay, reconciliation
    |
    +-- rtdb-typed      typed Serde CRUD, queries, realtime events
    |
    +-- rtdb-rs         REST + SSE transport
            |
            v
    Firebase Realtime Database Emulator
```

The goal is to validate the ecosystem as one application stack, not merely four crates that pass their own unit tests.

This plan is intentionally aggressive. It is designed to answer four questions before coordinated publication:

1. Can all four crates operate together under realistic mixed application load?
2. Does state remain correct through sustained concurrency, reconnect churn, offline operation, crashes, and replay?
3. Where does the current stack begin to degrade in latency, memory, CPU, queue depth, or reliability?
4. Can we publish a conservative, measured local validation envelope without pretending the local emulator is equivalent to Firebase production infrastructure?

## Claim boundary

These tests are **production-style local stress tests**, not proof of Firebase production-service capacity.

The Firebase Emulator, local loopback network, host CPU, JVM, filesystem, Rust allocator, OS scheduler, and test machine all affect results. Therefore benchmark results may be reported as:

> Tested locally with X synchronized paths, Y concurrent writers, Z queued durable mutations, N reconnect/restart cycles, and final convergence verified.

Do not report:

> Supports X production users.

Do not claim a universal maximum throughput from one workstation.

The most important result is correctness under pressure. Throughput numbers are secondary unless final convergence and durability guarantees remain intact.

## Test ownership

`rtdb-sync` is the orchestration home for this plan because it is the highest-level state-management crate and already owns emulator, recovery, persistence, and offline acceptance infrastructure.

Each crate remains responsible for its own semantics:

- `rtdb-rs`: HTTP transport, Firebase paths/query encoding, SSE lifecycle, REST request correctness
- `rtdb-typed`: typed serialization/deserialization, typed CRUD/query/event conversion
- `rtdb-admin`: service-account loading, OAuth exchange, token caching/refresh, client replacement
- `rtdb-sync`: state hydration, watches, local writes, reconnect, persistence, offline queue, replay, acknowledgement, reconciliation

The stress harness must not duplicate production implementations merely to make testing easier.

## Environment safety requirements

All Firebase emulator tests must remain restricted to a `demo-*` Firebase project ID.

Default target:

```text
FIREBASE_PROJECT_ID=demo-rtdb-sync
FIREBASE_DATABASE_EMULATOR_HOST=127.0.0.1:9000
```

Requirements:

- refuse to run destructive load profiles against non-`demo-*` project IDs
- never require production Firebase credentials for emulator stress
- isolate each run beneath a unique RTDB root/namespace
- remove temporary persistence stores after successful or failed runs
- keep durable offline test credentials/tokens outside persisted synchronization data
- record exact git commit SHAs and crate versions used by each result
- record host hardware/software metadata for benchmark comparison

## Full-stack harness architecture

The stress application should behave like a real application rather than directly invoking internal functions.

```text
                         mixed workload generator
                                  |
       +--------------------------+--------------------------+
       |                          |                          |
 direct raw traffic          typed traffic             synchronized traffic
       |                          |                          |
    rtdb-rs                  rtdb-typed                  rtdb-sync
       |                          |                          |
       +--------------------------+--------------------------+
                                  |
                        authenticated client path
                                  |
                              rtdb-admin
                                  |
                                  v
                        Firebase RTDB Emulator
```

The harness should intentionally mix abstraction levels. Real systems may have one component writing through `rtdb-sync`, another service using `rtdb-typed`, and diagnostic or administrative code using `rtdb-rs` directly.

## rtdb-admin integration limitation

The Firebase RTDB Emulator does not reproduce the full Google service-account OAuth token exchange path.

Therefore `rtdb-admin` requires two complementary test paths:

### Auth stress path

Use the existing controlled/mock OAuth infrastructure to stress:

- concurrent token requests
- single-flight token refresh
- expiry and refresh skew
- service-account rotation
- invalid credentials
- malformed OAuth responses
- HTTP failures
- timeouts
- repeated refresh failures
- client replacement while callers are active
- shutdown during refresh
- many independent identities if `CredentialManager` is used

### Full-stack handoff path

Use `rtdb-admin` to construct/update the same `rtdb-rs` client shape consumed by the rest of the ecosystem, then drive the RTDB emulator workload with that client.

The goal is to prove the auth/client lifecycle boundary integrates correctly even though Google OAuth itself is mocked locally.

## Required measurements

Every heavy profile should emit machine-readable and human-readable results.

Minimum measurements:

```text
operations attempted
operations succeeded
operations failed
writes/sec
events/sec
replay mutations/sec

p50 operation latency
p95 operation latency
p99 operation latency
maximum latency

Rust process RSS
Rust process CPU
Firebase Emulator RSS
Firebase Emulator CPU
open connections when measurable
active sync tasks
active subscribers

pending mutation count
maximum pending mutation count
oldest pending mutation age
journal size
snapshot size
snapshot persistence latency
journal append latency
journal compaction latency

reconnect attempts
reconnect successes
reconnect failures
time to reconnect
time from reconnect to current state
replay attempts
replay successes
replay failures
conflict count
persistence failures

final convergence mismatches
unexpected task leaks
unexpected pending mutations after completion
```

If percentile instrumentation is not yet available, raw latency samples may be recorded and summarized after the run.

## Machine baseline

Every published benchmark result must include enough host information to reproduce or contextualize it:

```text
OS and kernel
CPU model / logical core count
RAM
storage type
Rust version
Java version
Node version
firebase-tools version
crate commit SHAs / versions
release or debug build
benchmark duration
```

Performance numbers from debug builds must never be presented as release-profile throughput.

## Core correctness invariant

Every profile that permits eventual recovery must end with:

```text
for every synchronized test path:

local typed state == Firebase emulator state
```

Additional completion invariants:

```text
pending durable mutations == 0
acknowledged mutations are not replayed indefinitely
journal is compacted according to policy
all expected sync handles reach current/connected state
all requested tasks shut down
no cross-namespace events are observed
no silently lost durable mutation
```

A test run that achieves high throughput but violates convergence is a failure.

## Profile 1 - standard integrated application load

Purpose: establish a repeatable baseline that should pass on every release candidate.

Target workload:

- 100 synchronized typed paths
- at least 100 active sync handles
- 2 subscribers per path where practical
- 10 local writer tasks
- 10 remote writer tasks
- direct raw `rtdb-rs` traffic mixed with `rtdb-typed` traffic
- PUT/PATCH/delete/recreate operations
- at least 10,000 total mutations
- bounded runtime of roughly 5-10 minutes

Required assertions:

- zero unexplained operation loss
- zero final convergence mismatches
- no orphan synchronization tasks
- no cross-namespace observations
- bounded pending queue after writers stop
- pending queue drains completely

## Profile 2 - fan-out pressure

Purpose: test watch/subscriber delivery pressure separately from write pressure.

Progressive targets:

```text
100 subscribers
500 subscribers
1,000 subscribers
then increase until degradation is measurable
```

Workload:

- one or a small set of hot synchronized paths
- continuous sequential and burst mutations
- intentionally slow subscriber subgroup
- subscriber churn during active writes

Measure:

- delivery latency
- lag/backpressure behavior
- memory growth
- whether slow consumers affect unrelated consumers
- whether every surviving subscriber converges to final state

## Profile 3 - multi-path synchronization scale

Purpose: find the practical local envelope for simultaneous synchronized RTDB paths.

Progression:

```text
100 paths
250 paths
500 paths
1,000 paths
then increase in controlled steps
```

Each path receives periodic remote and local mutations.

Measure:

- memory per active sync handle
- CPU cost as path count increases
- connection/SSE behavior
- reconnect recovery time
- final convergence

Do not jump immediately to an extreme count. Record the curve so the degradation point is visible.

## Profile 4 - concurrent mixed writers

Purpose: stress bidirectional synchronization and crate boundary interactions.

Sources of writes:

- local optimistic writes through `rtdb-sync`
- confirmed writes through `rtdb-sync`
- typed CRUD through `rtdb-typed`
- direct REST writes through `rtdb-rs`

Targets:

- independent paths
- overlapping parent/child paths where supported
- deliberately hot shared paths

Progressive writer counts:

```text
20 concurrent writers
50 concurrent writers
100 concurrent writers
200+ if stable
```

Assertions:

- explicit conflict policy is followed
- no self-echo loops
- independent pending mutations are retained correctly
- failures do not erase unrelated queued mutations
- acknowledgements correspond to the intended operations
- final convergence succeeds

## Profile 5 - offline backlog and replay

Purpose: validate the main 0.4.0 durability value proposition under substantial queue pressure.

Baseline progression:

```text
1,000 queued durable mutations
10,000 queued durable mutations
50,000 queued durable mutations
100,000 if the previous level remains practical
```

Scenario:

```text
start emulator
hydrate synchronized state
persist snapshot
stop emulator
continue accepted offline writes
verify durable journal growth
kill Rust process without graceful shutdown
restart Rust while emulator is still unavailable
restore snapshot and pending journal
continue offline writes
restart emulator
inject remote mutations during replay
allow reconnect + reconciliation + replay
wait for current state
verify final convergence
verify pending == 0
verify compaction
```

Record:

- restore duration
- replay throughput
- peak queue depth
- peak RSS
- journal growth per mutation
- compaction duration
- time to confirmed-current state

## Profile 6 - crash-window matrix

Purpose: deliberately terminate the process at dangerous durability boundaries.

Required crash points:

- before snapshot persistence
- during/around snapshot replacement where injectable
- before journal append
- immediately after journal append
- after optimistic local apply
- before remote send
- after remote success but before durable acknowledgement
- during acknowledgement persistence
- during journal compaction
- during replay
- during reconnect backoff

For every recoverable crash point:

- restart from the same durable store
- verify no durable pending mutation disappeared silently
- verify no acknowledged mutation is resurrected due only to compaction corruption
- verify state either converges or enters an explicit visible failure/conflict state

## Profile 7 - emulator outage and reconnect storm

Purpose: stress the 0.2 reconnect machinery while 0.4 durable writes are active.

Suggested sequence:

```text
64-250 active paths
continuous writers
kill emulator
wait random bounded duration
restart emulator
wait for all paths to recover
repeat 25 cycles
```

Heavier manual run:

```text
500+ active paths
50+ writers
50 outage/recovery cycles
```

Assertions:

- no duplicate long-lived stream tasks
- cancellation remains responsive
- backoff does not spin
- all surviving paths rehydrate before being marked current
- pending durable writes replay once connectivity returns
- memory does not grow without bound per cycle

## Profile 8 - auth/token lifecycle storm

Purpose: break `rtdb-admin` under concurrency separately from emulator transport.

Required scenarios:

- 100+ concurrent callers requesting the same token near expiry
- ensure one refresh operation is shared when single-flight semantics apply
- repeated credential rotation while callers are active
- multiple identities refreshing independently
- mock 429 responses
- mock 5xx responses
- network timeout simulation
- malformed token responses
- expiry races
- shutdown during refresh
- repeated invalidation + refresh

Record:

- refresh attempts
- actual OAuth exchanges
- waiter count where observable
- token reuse ratio
- refresh latency
- failed refreshes
- caller-visible errors

The test fails if concurrent callers cause uncontrolled duplicate refresh storms contrary to the documented API semantics.

## Profile 9 - authenticated-client replacement during live sync

Purpose: prove the `rtdb-admin` -> `rtdb-rs` -> `rtdb-typed` -> `rtdb-sync` handoff under active workloads.

Scenario:

- run active synchronized paths
- run continuous local and remote writes
- rotate service-account source in the controlled auth test environment
- refresh or replace the `rtdb-rs` client
- allow sync reconnect/replacement hooks to consume the new client
- continue workload without resetting application state

Assertions:

- no credential lifecycle logic leaks into `rtdb-sync`
- client replacement does not corrupt synchronized state
- reconnect succeeds
- final convergence succeeds

## Profile 10 - persistence I/O pressure

Purpose: determine whether disk persistence becomes the bottleneck before network/sync semantics do.

Test with:

- large snapshots
- many small journal records
- burst writes
- frequent acknowledgement updates
- compaction while normal synchronization continues

Record separately:

- synchronization task CPU
- persistence worker CPU
- write latency
- fsync behavior if applicable
- journal size
- snapshot size
- time blocked on persistence

Persistence work must not indefinitely stall the main synchronization task.

## Profile 11 - namespace and multi-tenant isolation

Purpose: verify isolation under load.

Run many independent namespaces concurrently with intentionally similar path structures.

Assertions:

- no event from namespace A appears in namespace B
- no persistence key collision across namespaces
- no journal replay crosses namespace boundaries
- shutdown/clear for one namespace does not affect another

## Profile 12 - churn and lifecycle abuse

Purpose: find task/resource leaks.

Run repeated cycles of:

```text
create sync handles
attach subscribers
write
cancel some handles
restart some handles
replace clients
remove subscribers
shutdown
repeat
```

Targets:

- 1,000 lifecycle operations minimum
- larger manual runs if memory remains stable

Measure RSS at fixed intervals and after forced idle periods.

Memory growth alone is not automatically a leak because allocators may retain pages, but active task/subscriber/resource counts must return to their expected baseline.

## Profile 13 - sustained soak

Purpose: expose slow leaks, rare races, retry accumulation, and journal problems that short tests cannot reveal.

Suggested workload:

- 250-500 active paths
- mixed local/remote writers
- continuous typed reads
- periodic direct raw writes
- periodic client/token replacement
- periodic emulator outage
- periodic offline mutation burst
- occasional process restart with durable recovery

Durations:

```text
30 minutes - development soak
2 hours    - release-candidate soak
8+ hours   - milestone/manual soak
```

Required outcome:

- no unrecovered divergence
- no unbounded queue growth when connectivity is healthy
- no task count drift
- no persistent reconnect storm
- no journal corruption
- final convergence succeeds after workload stops

## Profile 14 - break-point / find-the-wall test

Purpose: determine where the current architecture or local test environment stops behaving acceptably.

Do not choose a single arbitrary target and stop when it passes.

Increase one dimension at a time:

```text
sync paths
subscribers
writers
write rate
offline queue size
reconnect frequency
```

Stop escalation when one of these occurs:

- correctness failure
- final convergence failure
- sustained queue growth after writers stabilize
- severe p99 latency increase
- memory exceeds a predefined safe fraction of host RAM
- Rust CPU saturates
- emulator JVM CPU saturates
- emulator becomes unable to serve the target workload
- repeated connection failure prevents stable recovery

When a break point occurs, identify the bottleneck as closely as possible:

```text
Rust sync engine
Rust persistence
Rust transport
Rust typed conversion
admin/token path
Firebase Emulator JVM
host CPU
host memory
host storage
OS/socket limits
```

A local emulator limit must not be mislabeled as an `rtdb-sync` limit.

## Standard / heavy / soak / break-point tiers

### Standard release profile

Run for every release candidate:

```text
100 synchronized paths
20 mixed writers
10,000+ total mutations
standard emulator suite
restart/recovery suite
durable offline acceptance
full final convergence verification
```

### Heavy milestone profile

Run before milestone publication and after major synchronization changes:

```text
500-1,000 synchronized paths
50-100 mixed writers
100,000+ total operations
10,000-50,000 offline queued mutations
multiple emulator restart cycles
client replacement during workload
30-60 minute duration
```

### Soak profile

Run manually before coordinated ecosystem release:

```text
250-500 synchronized paths
realistic mixed workload
periodic outage
periodic offline queueing
periodic process restart
2-8+ hour duration
```

### Break-point profile

Run manually to establish the tested local envelope:

```text
progressively increase load until a measurable limit appears
record both Rust and emulator saturation
never publish the break point as a universal production maximum
```

## Separate process monitoring

The benchmark must monitor the Rust harness and Firebase Emulator independently.

Otherwise a slowdown cannot be attributed correctly.

At minimum capture periodic samples of:

```text
Rust PID CPU / RSS
Firebase Emulator PID CPU / RSS
system load
available memory
```

Where practical also capture disk throughput and open socket counts.

## Result artifact

Each manual heavy/soak/break-point run should produce a timestamped result file containing:

```text
benchmark profile
crate versions / commit SHAs
machine baseline
configuration
start/end time
duration
operation counts
latency percentiles
throughput
resource peaks
reconnect metrics
offline/replay metrics
conflicts
failures
final convergence result
identified bottleneck if any
```

JSON is preferred for machine comparison, with an optional Markdown summary for human review.

Benchmark output should not contain service-account private keys, OAuth tokens, synchronized secrets, or unredacted credentials.

## Failure policy

A heavy test failure should be classified before changing code.

Categories:

```text
correctness bug
race/deadlock
resource leak
persistence durability bug
replay/acknowledgement bug
conflict-policy bug
crate integration bug
emulator limitation
host resource limitation
benchmark harness bug
expected external dependency/package blocker
```

Do not weaken an assertion merely to make a load profile green unless the documented guarantee itself was incorrect and is intentionally changed.

## Release blocking failures

The following block `rtdb-sync 0.4.0` / coordinated ecosystem publication:

- reproducible lost durable mutation
- silent state divergence after recovery
- journal corruption under supported operations
- namespace cross-contamination
- acknowledgement corruption causing endless replay
- crash recovery labeling stale state as confirmed-current incorrectly
- deterministic deadlock
- synchronization task leak reproduced under bounded lifecycle churn
- client replacement corrupting state
- service-account/token lifecycle responsibility leaking into sync implementation
- final standard integrated profile failing convergence
- durable offline acceptance failing

Performance degradation alone is not automatically release-blocking if it occurs beyond the documented tested envelope and correctness remains intact. It must still be documented.

## Final coordinated-release validation

After `rtdb-rs 0.3.2` is published and all development git/path overrides are removed, rerun the ecosystem using the exact crates.io dependency graph users will receive.

Required sequence:

```text
1. rtdb-rs release gates
2. rtdb-typed release gates against published rtdb-rs
3. rtdb-admin release gates against published rtdb-rs
4. rtdb-sync release gates against published lower layers
5. standard integrated ecosystem stress profile
6. restart/recovery profile
7. durable offline acceptance
8. auth/token lifecycle stress
9. authenticated-client replacement integration
10. heavy milestone profile
11. release-candidate soak
12. final convergence audit
13. cargo package / cargo publish --dry-run for each publishable crate
```

The full four-crate ecosystem is considered stress-validated for the coordinated release only when the same versions intended for publication are the versions used by the final integrated test.

## Suggested implementation layout

Keep normal correctness CI fast. Do not force the full heavy/soak workload on every push.

Suggested repository structure:

```text
scripts/
  test-emulator.sh
  test-emulator-recovery.sh
  test-emulator-offline.sh
  test-ecosystem-stress.sh
  test-ecosystem-heavy.sh
  test-ecosystem-soak.sh
  test-ecosystem-breakpoint.sh

tests or stress harness module/
  integrated_standard
  integrated_mixed_writers
  integrated_offline_backlog
  integrated_client_replacement
  integrated_namespace_isolation
  integrated_churn
  integrated_soak
```

The exact implementation may differ. The important requirement is reproducibility and separation between normal CI correctness tests and manually invoked machine-dependent load tests.

## Definition of done

The ecosystem stress initiative is complete when all of the following are true:

- all four crates participate in the integrated validation path where their real responsibility applies
- real local Firebase RTDB Emulator traffic is used for transport/typed/sync testing
- controlled OAuth infrastructure is used for admin token lifecycle stress
- the admin-created/replaced client participates in an integrated RTDB workload
- standard, heavy, soak, and break-point profiles exist
- Rust and emulator resource usage are measured independently
- durable offline backlog survives process death and replays successfully
- mixed direct/typed/sync writers converge
- repeated emulator outages recover
- namespace isolation holds under load
- final local typed state equals emulator state for every test namespace/path
- pending durable queues drain after successful recovery
- result artifacts identify exact crate commits and machine configuration
- no benchmark result is presented as a universal Firebase production capacity guarantee

The target is not to prove that the ecosystem can never fail. The target is to deliberately find failure boundaries, fix correctness defects, document measured limits, and ship with a defensible tested envelope.

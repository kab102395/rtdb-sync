# Environment and stress testing

## Test layers

1. Pure unit tests: state transitions, patch application, null/deletion semantics, subscriber behavior, cancellation, and retry calculations. No network.
2. Local mock tests: deterministic initial GET/SSE sequences, malformed events, disconnects, delayed events, cancellation, and conversion failures.
3. Firebase Realtime Database emulator: official local behavior for hydration, realtime delivery, namespace isolation, reconnect, and concurrent writers.
4. Manual heavy profiles: larger local stress runs used to find races/leaks; these are not production throughput benchmarks.
5. Durable offline acceptance: separate test processes share a file journal while the local RTDB emulator is stopped and restarted.

## Emulator environment

Use only a `demo-*` Firebase project ID. Default target is `demo-rtdb-sync` on `127.0.0.1:9000`, with Emulator UI on `127.0.0.1:4000`. No production Firebase credentials belong in normal tests.

The implementation should reuse `rtdb-rs` namespace support so multiple test namespaces can share one emulator process without state leakage.

## 0.1.0 stress requirements

Standard profile:

- 32 synchronized paths
- 32 active subscribers minimum
- 50 remote mutations per path
- mixed full PUT, partial PATCH, and delete/recreate cycles
- verify final local state equals final emulator state for every path
- verify every synchronization task shuts down
- verify no cross-namespace observations

Fan-out profile:

- one shared RTDB path
- 64 local subscribers
- 250 sequential mutations
- every subscriber must converge on the same final generation/state

Churn profile:

- repeatedly create and stop 100 synchronization tasks
- no orphan background tasks
- no deadlocked subscriber channels
- bounded memory growth under repeated lifecycle churn

Manual heavy profile:

- 64 synchronized paths
- 100 mutations per path
- repeat for multiple runs with unique roots

## 0.2.0 stress requirements

Add controlled disconnect/reconnect scenarios:

- 32 paths active while emulator is interrupted and restored
- randomized stream termination in mock transport
- 25 reconnect cycles per selected path
- cancellation during backoff
- token/client replacement during reconnect
- rehydration must converge before state is reported current
- no duplicate long-lived stream tasks after recovery

Manual heavy resilience profile:

- 64 paths
- 10 outage/recovery cycles
- 100 mutations per path across the run

## 0.3.0 stress requirements

Bidirectional conflict profile:

- 32 synchronized paths
- concurrent local and remote writers
- at least 100 mixed writes per path
- induced write failures and delayed acknowledgements
- reconnect while writes are pending
- verify documented conflict policy, rollback behavior, queue bounds, and final convergence

Fan-in profile:

- multiple local producers writing through one synchronized state object
- concurrent Firebase remote writes
- verify no lost local acknowledgements and no self-echo loops

## What stress results mean

These tests are functional concurrency and correctness stress tests. They should report operation counts, failures, reconnects, and elapsed time for diagnosis, but they must not be advertised as maximum production capacity or latency benchmarks.

## Release discipline

For every release candidate: run formatting, clippy with warnings denied, all deterministic tests, package verification, publish dry-run, then the standard emulator stress profile and the durable offline acceptance. Heavy profiles are required before milestone releases and whenever synchronization internals materially change.

## Current evidence

The repository now includes deterministic unit and mock suites, official
emulator CRUD/SSE and namespace tests, a 32-path remote-mutation profile, a
64-path heavy profile, 64-subscriber fan-out, 100-task lifecycle churn, typed
conversion failure coverage, bidirectional local/remote conflict and fan-in
profiles, and a real emulator restart harness. The manual resilience runner
executes 10 outage/recovery cycles with 64 paths per cycle and waits for every
stream to reconnect before checking convergence.

The latest local run passed 23 deterministic tests, the complete standard
emulator suite, the 32-path restart/recovery harness, and the durable
three-process offline acceptance. The 64-path resilience runner has also
passed 10 outage/recovery cycles. These are correctness and lifecycle tests,
not production capacity
benchmarks. The bidirectional failure/delayed-ack behavior is covered by the
injected mock failure and rollback tests; a real Firebase service cannot inject
those transport faults without an external fault proxy.

## Known release limitation

The durable acceptance flow is reproducible with:

```text
./scripts/test-emulator-offline.sh
```

It persists a live emulator snapshot, stops the emulator, queues an optimistic
mutation while the database is unreachable, starts a new test process with the
same store, and verifies replay plus journal compaction against the emulator.

The file backend is intentionally plaintext. It restricts its directory to
0700 and records to 0600 on Unix, but those permissions are not encryption.
Applications storing secrets or regulated data must provide an encrypted
backend and must keep credentials/tokens outside the persistence API.

`cargo fmt`, `cargo test --all-targets --all-features`, and clippy with denied
warnings pass. `cargo package`/publish dry-run cannot complete until the
git-pinned `rtdb-rs` 0.3.2 release is available on crates.io; the package
metadata now carries the correct upstream versions rather than silently
claiming an older transport API.

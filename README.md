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

## 0.1.0 target

The first release is intentionally one-way: Firebase -> synchronized Rust state.

Planned foundation:

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

Bidirectional local writes, optimistic updates, and conflict handling are deferred until the state/event semantics are proven.

See `docs/ROADMAP.md` and `docs/ENVIRONMENT_AND_STRESS_TESTING.md`.

## Status

Pre-0.1.0 scaffold. Public API is not stable yet.

## License

MIT

//! Realtime synchronized Rust state for Firebase Realtime Database.
//!
//! `rtdb-sync` owns local synchronization semantics.  A [`Backend`] supplies
//! hydration, realtime events, and writes; the Firebase REST/SSE implementation
//! is provided as [`FirebaseBackend`].

use async_trait::async_trait;
use futures_core::Stream;
use futures_util::StreamExt;
use rtdb_rs::{RtdbClient, RtdbEvent};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};
use std::{
    fmt,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub type EventStream = Pin<Box<dyn Stream<Item = Result<Event, SyncError>> + Send>>;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Event {
    Put {
        path: String,
        data: Value,
    },
    Patch {
        path: String,
        data: Map<String, Value>,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SyncError {
    #[error("backend: {0}")]
    Backend(String),
    #[error("invalid event path: {0}")]
    InvalidPath(String),
    #[error("conversion failed: {0}")]
    Conversion(String),
    #[error("synchronization cancelled")]
    Cancelled,
    #[error("conflict: {0}")]
    Conflict(String),
}

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    async fn get(&self, path: &str) -> Result<Value, SyncError>;
    async fn subscribe(&self, path: &str) -> Result<EventStream, SyncError>;
    async fn put(&self, path: &str, value: Value) -> Result<(), SyncError>;
    async fn patch(&self, path: &str, value: Map<String, Value>) -> Result<(), SyncError>;
    async fn before_reconnect(&self, _attempt: usize) -> Result<(), SyncError> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub generation: u64,
    pub value: Value,
}

impl Snapshot {
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, SyncError> {
        serde_json::from_value(self.value.clone()).map_err(|e| SyncError::Conversion(e.to_string()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryPolicy {
    Never,
    Exponential {
        max_attempts: Option<usize>,
        base: Duration,
        max: Duration,
    },
}

impl RetryPolicy {
    pub fn delay(&self, attempt: usize, jitter_max: Duration) -> Option<Duration> {
        match *self {
            RetryPolicy::Never => None,
            RetryPolicy::Exponential {
                max_attempts,
                base,
                max,
            } => {
                if max_attempts.map(|n| attempt > n).unwrap_or(false) {
                    return None;
                }
                let factor = 2u32.saturating_pow(attempt.saturating_sub(1) as u32);
                let backoff = base.saturating_mul(factor).min(max);
                let jitter = if jitter_max.is_zero() {
                    Duration::ZERO
                } else {
                    let seed = (attempt as u64)
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    Duration::from_nanos(seed % (jitter_max.as_nanos() as u64 + 1))
                };
                Some(
                    backoff
                        .saturating_add(jitter)
                        .min(max.saturating_add(jitter_max)),
                )
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePolicy {
    Confirmed,
    Optimistic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictPolicy {
    RemoteWins,
    LocalWins,
    Reject,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub retry: RetryPolicy,
    pub write_policy: WritePolicy,
    pub jitter_max: Duration,
    pub conflict_policy: ConflictPolicy,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            retry: RetryPolicy::Exponential {
                max_attempts: None,
                base: Duration::from_millis(50),
                max: Duration::from_secs(5),
            },
            write_policy: WritePolicy::Confirmed,
            jitter_max: Duration::ZERO,
            conflict_policy: ConflictPolicy::RemoteWins,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub reconnect_attempts: u64,
    pub stream_failures: u64,
    pub hydration_failures: u64,
    pub successful_writes: u64,
    pub failed_writes: u64,
}

#[derive(Default)]
struct Metrics {
    reconnect_attempts: AtomicU64,
    stream_failures: AtomicU64,
    hydration_failures: AtomicU64,
    successful_writes: AtomicU64,
    failed_writes: AtomicU64,
}
impl Metrics {
    fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            reconnect_attempts: self.reconnect_attempts.load(Ordering::Relaxed),
            stream_failures: self.stream_failures.load(Ordering::Relaxed),
            hydration_failures: self.hydration_failures.load(Ordering::Relaxed),
            successful_writes: self.successful_writes.load(Ordering::Relaxed),
            failed_writes: self.failed_writes.load(Ordering::Relaxed),
        }
    }
}

pub struct SyncHandle {
    snapshot: watch::Receiver<Snapshot>,
    status: watch::Receiver<SyncStatus>,
    cancel: CancellationToken,
    writes: mpsc::Sender<Write>,
    join: tokio::task::JoinHandle<()>,
    metrics: Arc<Metrics>,
}

enum Write {
    Put(
        String,
        Value,
        tokio::sync::oneshot::Sender<Result<(), SyncError>>,
    ),
    Patch(
        String,
        Map<String, Value>,
        tokio::sync::oneshot::Sender<Result<(), SyncError>>,
    ),
}

struct PendingMutation {
    path: String,
    event: Event,
}

impl SyncHandle {
    pub fn snapshot(&self) -> Snapshot {
        self.snapshot.borrow().clone()
    }
    pub fn subscribe(&self) -> watch::Receiver<Snapshot> {
        self.snapshot.clone()
    }
    pub fn status(&self) -> SyncStatus {
        self.status.borrow().clone()
    }
    pub fn subscribe_status(&self) -> watch::Receiver<SyncStatus> {
        self.status.clone()
    }
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }
    pub async fn put<T: Serialize>(
        &self,
        path: impl Into<String>,
        value: T,
    ) -> Result<(), SyncError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let v = serde_json::to_value(value).map_err(|e| SyncError::Conversion(e.to_string()))?;
        self.writes
            .send(Write::Put(path.into(), v, tx))
            .await
            .map_err(|_| SyncError::Cancelled)?;
        rx.await.map_err(|_| SyncError::Cancelled)?
    }
    pub async fn patch<T: Serialize>(
        &self,
        path: impl Into<String>,
        value: T,
    ) -> Result<(), SyncError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let v = serde_json::to_value(value).map_err(|e| SyncError::Conversion(e.to_string()))?;
        let data = v
            .as_object()
            .cloned()
            .ok_or_else(|| SyncError::Conversion("patch must be an object".into()))?;
        self.writes
            .send(Write::Patch(path.into(), data, tx))
            .await
            .map_err(|_| SyncError::Cancelled)?;
        rx.await.map_err(|_| SyncError::Cancelled)?
    }
    pub async fn shutdown(self) {
        self.cancel.cancel();
        let _ = self.join.await;
    }
}

pub fn start<B: Backend>(backend: Arc<B>, path: impl Into<String>, config: Config) -> SyncHandle {
    let path = path.into();
    let (snap_tx, snap_rx) = watch::channel(Snapshot {
        generation: 0,
        value: Value::Null,
    });
    let (status_tx, status_rx) = watch::channel(SyncStatus::Idle);
    let cancel = CancellationToken::new();
    let (writes, mut write_rx) = mpsc::channel(64);
    let child = cancel.clone();
    let metrics = Arc::new(Metrics::default());
    let task_metrics = metrics.clone();
    let join = tokio::spawn(async move {
        #[allow(unused_assignments)]
        let mut state: Option<Value> = None;
        let mut generation = 0;
        let mut attempt = 0usize;
        let mut pending = Vec::new();
        let _ = status_tx.send(SyncStatus::Hydrating);
        loop {
            let hydrated = tokio::select! { _ = child.cancelled() => { let _ = status_tx.send(SyncStatus::Stopped); return; }, result = backend.get(&path) => result };
            match hydrated {
                Ok(value) => {
                    state = Some(value);
                    let state_ref = state.as_ref().expect("hydration set state");
                    generation += 1;
                    let _ = snap_tx.send(Snapshot {
                        generation,
                        value: state_ref.clone(),
                    });
                    attempt = 0;
                }
                Err(e) => {
                    task_metrics
                        .hydration_failures
                        .fetch_add(1, Ordering::Relaxed);
                    if !retry(
                        &*backend,
                        &config,
                        &mut attempt,
                        &child,
                        &status_tx,
                        &task_metrics,
                    )
                    .await
                    {
                        let _ = status_tx.send(SyncStatus::Failed(e));
                        return;
                    }
                    continue;
                }
            }
            let mut stream = match backend.subscribe(&path).await {
                Ok(s) => s,
                Err(e) => {
                    if !retry(
                        &*backend,
                        &config,
                        &mut attempt,
                        &child,
                        &status_tx,
                        &task_metrics,
                    )
                    .await
                    {
                        let _ = status_tx.send(SyncStatus::Failed(e));
                        return;
                    }
                    continue;
                }
            };
            let _ = status_tx.send(SyncStatus::Connected);
            loop {
                tokio::select! {
                    _ = child.cancelled() => { let _ = status_tx.send(SyncStatus::Stopped); return; },
                Some(write) = write_rx.recv() => { let result = handle_write(&*backend, &path, state.as_mut().expect("hydrated"), &mut generation, &snap_tx, &mut pending, write, config.write_policy).await; if result.is_ok() { task_metrics.successful_writes.fetch_add(1, Ordering::Relaxed); } else { task_metrics.failed_writes.fetch_add(1, Ordering::Relaxed); let _ = status_tx.send(SyncStatus::Failed(result.clone().unwrap_err())); } },
                event = stream.next() => match event { Some(Ok(event)) => match reconcile_event(&path, state.as_mut().expect("hydrated"), &mut generation, &snap_tx, &mut pending, event, config.conflict_policy) { Ok(()) => {}, Err(e) => { let _ = status_tx.send(SyncStatus::Failed(e)); return; } }, Some(Err(_)) | None => break }
                }
            }
            task_metrics.stream_failures.fetch_add(1, Ordering::Relaxed);
            if !retry(
                &*backend,
                &config,
                &mut attempt,
                &child,
                &status_tx,
                &task_metrics,
            )
            .await
            {
                let _ = status_tx.send(SyncStatus::Stopped);
                return;
            }
        }
    });
    SyncHandle {
        snapshot: snap_rx,
        status: status_rx,
        cancel,
        writes,
        join,
        metrics,
    }
}

async fn retry<B: Backend + ?Sized>(
    backend: &B,
    config: &Config,
    attempt: &mut usize,
    cancel: &CancellationToken,
    status: &watch::Sender<SyncStatus>,
    metrics: &Metrics,
) -> bool {
    *attempt += 1;
    let Some(delay) = config.retry.delay(*attempt, config.jitter_max) else {
        return false;
    };
    metrics.reconnect_attempts.fetch_add(1, Ordering::Relaxed);
    let _ = status.send(SyncStatus::Reconnecting {
        attempt: *attempt,
        delay,
    });
    if backend.before_reconnect(*attempt).await.is_err() {
        return false;
    }
    tokio::select! { _ = cancel.cancelled() => false, _ = tokio::time::sleep(delay) => true }
}

#[allow(clippy::too_many_arguments)]
async fn handle_write<B: Backend + ?Sized>(
    backend: &B,
    root: &str,
    state: &mut Value,
    generation: &mut u64,
    tx: &watch::Sender<Snapshot>,
    pending: &mut Vec<PendingMutation>,
    write: Write,
    policy: WritePolicy,
) -> Result<(), SyncError> {
    match write {
        Write::Put(path, value, done) => {
            let old = state.clone();
            let event = Event::Put {
                path: path.clone(),
                data: value.clone(),
            };
            pending.push(PendingMutation {
                path: path.clone(),
                event: event.clone(),
            });
            if policy == WritePolicy::Optimistic {
                apply_event(root, state, event.clone())?;
                *generation += 1;
                let _ = tx.send(Snapshot {
                    generation: *generation,
                    value: state.clone(),
                });
            }
            let result = backend.put(&join(root, &path)?, value).await;
            if result.is_err() && policy == WritePolicy::Optimistic {
                *state = old;
                *generation += 1;
                let _ = tx.send(Snapshot {
                    generation: *generation,
                    value: state.clone(),
                });
            }
            if result.is_err() {
                pending.retain(|mutation| mutation.path != path);
            }
            let _ = done.send(result.clone());
            result
        }
        Write::Patch(path, data, done) => {
            let old = state.clone();
            let event = Event::Patch {
                path: path.clone(),
                data: data.clone(),
            };
            pending.push(PendingMutation {
                path: path.clone(),
                event: event.clone(),
            });
            if policy == WritePolicy::Optimistic {
                apply_event(root, state, event.clone())?;
                *generation += 1;
                let _ = tx.send(Snapshot {
                    generation: *generation,
                    value: state.clone(),
                });
            }
            let result = backend.patch(&join(root, &path)?, data).await;
            if result.is_err() && policy == WritePolicy::Optimistic {
                *state = old;
                *generation += 1;
                let _ = tx.send(Snapshot {
                    generation: *generation,
                    value: state.clone(),
                });
            }
            if result.is_err() {
                pending.retain(|mutation| mutation.path != path);
            }
            let _ = done.send(result.clone());
            result
        }
    }
}

fn reconcile_event(
    root: &str,
    state: &mut Value,
    generation: &mut u64,
    tx: &watch::Sender<Snapshot>,
    pending: &mut Vec<PendingMutation>,
    event: Event,
    policy: ConflictPolicy,
) -> Result<(), SyncError> {
    if let Some(index) = pending
        .iter()
        .position(|mutation| mutation.path == event_path(&event))
    {
        let mutation = &pending[index];
        if equivalent_event(&mutation.event, &event) {
            pending.remove(index);
            return Ok(());
        }
        match policy {
            ConflictPolicy::LocalWins => return Ok(()),
            ConflictPolicy::Reject => {
                return Err(SyncError::Conflict(format!(
                    "remote event conflicts at {}",
                    event_path(&event)
                )))
            }
            ConflictPolicy::RemoteWins => {
                pending.remove(index);
            }
        }
    }
    apply_event(root, state, event)?;
    *generation += 1;
    let _ = tx.send(Snapshot {
        generation: *generation,
        value: state.clone(),
    });
    Ok(())
}

fn event_path(event: &Event) -> &str {
    match event {
        Event::Put { path, .. } | Event::Patch { path, .. } => path,
    }
}
fn equivalent_event(left: &Event, right: &Event) -> bool {
    match (left, right) {
        (Event::Put { path: a, data: b }, Event::Put { path: c, data: d }) => a == c && b == d,
        (Event::Patch { path: a, data: b }, Event::Patch { path: c, data: d }) => a == c && b == d,
        _ => false,
    }
}

fn join(root: &str, path: &str) -> Result<String, SyncError> {
    let a = root.trim_matches('/');
    let b = path.trim_matches('/');
    if !b.is_empty() && b.split('/').any(|x| x.is_empty() || x == "." || x == "..") {
        return Err(SyncError::InvalidPath(path.into()));
    }
    Ok(if b.is_empty() {
        a.into()
    } else if a.is_empty() {
        b.into()
    } else {
        format!("{a}/{b}")
    })
}
fn segments(path: &str) -> Result<Vec<&str>, SyncError> {
    let p = path.trim_matches('/');
    if p.is_empty() {
        Ok(vec![])
    } else {
        let out: Vec<_> = p.split('/').collect();
        if out.iter().any(|x| x.is_empty() || *x == "." || *x == "..") {
            Err(SyncError::InvalidPath(path.into()))
        } else {
            Ok(out)
        }
    }
}
pub fn apply_event(root: &str, state: &mut Value, event: Event) -> Result<(), SyncError> {
    let relative = match &event {
        Event::Put { path, .. } | Event::Patch { path, .. } => path.clone(),
    };
    let _ = join(root, &relative)?;
    let parts = segments(&relative)?;
    let target = get_or_create(state, &parts);
    match event {
        Event::Put { data, .. } => {
            if data.is_null() && !parts.is_empty() {
                remove_at(state, &parts);
            } else {
                *target = data;
            }
        }
        Event::Patch { data, .. } => {
            if data.is_empty() {
                return Ok(());
            }
            if !target.is_object() {
                *target = Value::Object(Map::new());
            }
            let obj = target.as_object_mut().unwrap();
            for (key, value) in data {
                let keys = segments(&key)?;
                set_at(obj, &keys, value);
            }
        }
    }
    Ok(())
}
fn get_or_create<'a>(value: &'a mut Value, parts: &[&str]) -> &'a mut Value {
    let mut current = value;
    for part in parts {
        if !current.is_object() {
            *current = Value::Object(Map::new());
        }
        current = current
            .as_object_mut()
            .unwrap()
            .entry(*part)
            .or_insert(Value::Null);
    }
    current
}
fn set_at(obj: &mut Map<String, Value>, parts: &[&str], value: Value) {
    if parts.is_empty() {
        return;
    }
    if parts.len() == 1 {
        if value.is_null() {
            obj.remove(parts[0]);
        } else {
            obj.insert(parts[0].into(), value);
        }
    } else {
        let child = obj
            .entry(parts[0])
            .or_insert_with(|| Value::Object(Map::new()));
        if !child.is_object() {
            *child = Value::Object(Map::new());
        }
        set_at(child.as_object_mut().unwrap(), &parts[1..], value);
    }
}
fn remove_at(value: &mut Value, parts: &[&str]) {
    if parts.is_empty() {
        *value = Value::Null;
        return;
    }
    if let Some(obj) = value.as_object_mut() {
        if parts.len() == 1 {
            obj.remove(parts[0]);
        } else if let Some(child) = obj.get_mut(parts[0]) {
            remove_at(child, &parts[1..]);
        }
    }
}

/// Marker used while the 0.1.0 synchronization API is being designed.
///
/// No stability guarantee is attached to this pre-release scaffold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncStatus {
    /// No synchronization task has started.
    Idle,
    /// Initial state is being hydrated.
    Hydrating,
    /// A realtime stream is active.
    Connected,
    /// The stream or hydration request is being retried.
    Reconnecting { attempt: usize, delay: Duration },
    /// Synchronization has stopped.
    Stopped,
    /// A non-recoverable backend, conversion, or event error occurred.
    Failed(SyncError),
}

impl fmt::Display for SyncStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Backend adapter that delegates REST, SSE, authentication parameters, and
/// emulator namespaces to the upstream `rtdb-rs` transport.
#[derive(Clone)]
pub struct RtdbBackend {
    client: Arc<RtdbClient>,
    base_url: String,
    token: String,
    namespace: Option<String>,
}

impl RtdbBackend {
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        let base_url = base_url.into();
        let token = token.into();
        Self {
            client: Arc::new(RtdbClient::new(&base_url, &token)),
            base_url,
            token,
            namespace: None,
        }
    }
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        self.client = Arc::new(
            RtdbClient::new(&self.base_url, &self.token).with_namespace(namespace.clone()),
        );
        self.namespace = Some(namespace);
        self
    }
    pub fn typed_client(&self) -> rtdb_typed::TypedClient {
        rtdb_typed::TypedClient::from_parts(&self.base_url, &self.token)
    }
    fn error(error: impl fmt::Debug) -> SyncError {
        SyncError::Backend(format!("{error:?}"))
    }
}

#[async_trait]
impl Backend for RtdbBackend {
    async fn get(&self, path: &str) -> Result<Value, SyncError> {
        self.client.get(path).await.map_err(Self::error)
    }
    async fn subscribe(&self, path: &str) -> Result<EventStream, SyncError> {
        let stream = self.client.stream(path).await.map_err(Self::error)?;
        let stream = stream.filter_map(|event| async move {
            match event {
                Ok(RtdbEvent::Put { path, data }) => Some(Ok(Event::Put { path, data })),
                Ok(RtdbEvent::Patch { path, data }) => Some(Ok(Event::Patch {
                    path,
                    data: data.as_object().cloned().unwrap_or_default(),
                })),
                Ok(RtdbEvent::KeepAlive) => None,
                Ok(RtdbEvent::Cancel) => {
                    Some(Err(SyncError::Backend("Firebase stream cancelled".into())))
                }
                Err(error) => Some(Err(Self::error(error))),
            }
        });
        Ok(Box::pin(stream))
    }
    async fn put(&self, path: &str, value: Value) -> Result<(), SyncError> {
        self.client
            .put(path, &value)
            .await
            .map(|_| ())
            .map_err(Self::error)
    }
    async fn patch(&self, path: &str, value: Map<String, Value>) -> Result<(), SyncError> {
        self.client
            .patch(path, &Value::Object(value))
            .await
            .map(|_| ())
            .map_err(Self::error)
    }
}

/// Firebase Realtime Database REST + Server-Sent Events backend.
#[derive(Clone)]
pub struct FirebaseBackend {
    client: reqwest::Client,
    base_url: String,
    bearer: Option<String>,
}
impl FirebaseBackend {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').into(),
            bearer: None,
        }
    }
    pub fn with_bearer(mut self, token: impl Into<String>) -> Self {
        self.bearer = Some(token.into());
        self
    }
    fn request(&self, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}/{}.json", self.base_url, path.trim_matches('/'));
        let req = self.client.get(url);
        if let Some(token) = &self.bearer {
            req.query(&[("access_token", token)])
        } else {
            req
        }
    }
    async fn check(response: reqwest::Response) -> Result<reqwest::Response, SyncError> {
        if response.status().is_success() {
            Ok(response)
        } else {
            Err(SyncError::Backend(format!("HTTP {}", response.status())))
        }
    }
}
#[async_trait]
impl Backend for FirebaseBackend {
    async fn get(&self, path: &str) -> Result<Value, SyncError> {
        let r = Self::check(
            self.request(path)
                .send()
                .await
                .map_err(|e| SyncError::Backend(e.to_string()))?,
        )
        .await?;
        r.json()
            .await
            .map_err(|e| SyncError::Backend(e.to_string()))
    }
    async fn subscribe(&self, path: &str) -> Result<EventStream, SyncError> {
        let url = format!("{}/{}.json", self.base_url, path.trim_matches('/'));
        let mut req = self.client.get(url).header("Accept", "text/event-stream");
        if let Some(token) = &self.bearer {
            req = req.query(&[("access_token", token)]);
        }
        let response = Self::check(
            req.send()
                .await
                .map_err(|e| SyncError::Backend(e.to_string()))?,
        )
        .await?;
        let mut bytes = response.bytes_stream();
        let stream = async_stream::try_stream! {
            let mut buffer = String::new();
            while let Some(chunk) = bytes.next().await {
                let chunk = chunk.map_err(|e| SyncError::Backend(e.to_string()))?;
                buffer.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(pos) = buffer.find("\n\n") {
                    let block = buffer.drain(..pos + 2).collect::<String>();
                    let mut kind = "put";
                    let mut data = None;
                    for line in block.lines() {
                        if let Some(v) = line.strip_prefix("event: ") { kind = v; }
                        if let Some(v) = line.strip_prefix("data: ") { data = Some(v); }
                    }
                    if let Some(raw) = data {
                        let v: Value = serde_json::from_str(raw).map_err(|e| SyncError::Backend(e.to_string()))?;
                        let path = v.get("path").and_then(Value::as_str).unwrap_or("").into();
                        let data = v.get("data").cloned().unwrap_or(Value::Null);
                        if kind == "patch" {
                            yield Event::Patch { path, data: data.as_object().cloned().unwrap_or_default() };
                        } else {
                            yield Event::Put { path, data };
                        }
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }
    async fn put(&self, path: &str, value: Value) -> Result<(), SyncError> {
        let url = format!("{}/{}.json", self.base_url, path.trim_matches('/'));
        let r = Self::check(
            self.client
                .put(url)
                .json(&value)
                .send()
                .await
                .map_err(|e| SyncError::Backend(e.to_string()))?,
        )
        .await?;
        let _ = r.bytes().await;
        Ok(())
    }
    async fn patch(&self, path: &str, value: Map<String, Value>) -> Result<(), SyncError> {
        let url = format!("{}/{}.json", self.base_url, path.trim_matches('/'));
        let r = Self::check(
            self.client
                .patch(url)
                .json(&value)
                .send()
                .await
                .map_err(|e| SyncError::Backend(e.to_string()))?,
        )
        .await?;
        let _ = r.bytes().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use serde::Deserialize;
    use std::sync::Mutex;

    #[derive(Clone)]
    struct Mock {
        value: Arc<Mutex<Value>>,
        events: Arc<Mutex<Vec<Event>>>,
        writes: Arc<Mutex<Vec<String>>>,
    }

    struct Flaky {
        subscriptions: AtomicU64,
        replacements: AtomicU64,
    }

    struct Failing;
    #[async_trait]
    impl Backend for Failing {
        async fn get(&self, _: &str) -> Result<Value, SyncError> {
            Ok(serde_json::json!({"count": 1}))
        }
        async fn subscribe(&self, _: &str) -> Result<EventStream, SyncError> {
            Ok(Box::pin(stream::pending()))
        }
        async fn put(&self, _: &str, _: Value) -> Result<(), SyncError> {
            Err(SyncError::Backend("injected write failure".into()))
        }
        async fn patch(&self, _: &str, _: Map<String, Value>) -> Result<(), SyncError> {
            Err(SyncError::Backend("injected write failure".into()))
        }
    }

    #[derive(Clone)]
    struct Live {
        value: Arc<Mutex<Value>>,
        events: tokio::sync::broadcast::Sender<Event>,
    }
    #[async_trait]
    impl Backend for Live {
        async fn get(&self, _: &str) -> Result<Value, SyncError> {
            Ok(self.value.lock().unwrap().clone())
        }
        async fn subscribe(&self, _: &str) -> Result<EventStream, SyncError> {
            let mut receiver = self.events.subscribe();
            Ok(Box::pin(async_stream::stream! {
                loop { match receiver.recv().await { Ok(event) => yield Ok(event), Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue, Err(tokio::sync::broadcast::error::RecvError::Closed) => break } }
            }))
        }
        async fn put(&self, _: &str, value: Value) -> Result<(), SyncError> {
            *self.value.lock().unwrap() = value.clone();
            let _ = self.events.send(Event::Put {
                path: "".into(),
                data: value,
            });
            Ok(())
        }
        async fn patch(&self, _: &str, value: Map<String, Value>) -> Result<(), SyncError> {
            let mut current = self.value.lock().unwrap();
            apply_event(
                "",
                &mut current,
                Event::Patch {
                    path: "".into(),
                    data: value.clone(),
                },
            )?;
            let _ = self.events.send(Event::Patch {
                path: "".into(),
                data: value,
            });
            Ok(())
        }
    }

    #[async_trait]
    impl Backend for Flaky {
        async fn get(&self, _: &str) -> Result<Value, SyncError> {
            Ok(Value::Null)
        }
        async fn subscribe(&self, _: &str) -> Result<EventStream, SyncError> {
            if self.subscriptions.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(Box::pin(stream::empty()))
            } else {
                Ok(Box::pin(stream::pending()))
            }
        }
        async fn put(&self, _: &str, _: Value) -> Result<(), SyncError> {
            Ok(())
        }
        async fn patch(&self, _: &str, _: Map<String, Value>) -> Result<(), SyncError> {
            Ok(())
        }
        async fn before_reconnect(&self, _: usize) -> Result<(), SyncError> {
            self.replacements.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[async_trait]
    impl Backend for Mock {
        async fn get(&self, _: &str) -> Result<Value, SyncError> {
            Ok(self.value.lock().unwrap().clone())
        }
        async fn subscribe(&self, _: &str) -> Result<EventStream, SyncError> {
            let events = std::mem::take(&mut *self.events.lock().unwrap());
            Ok(Box::pin(
                stream::iter(events.into_iter().map(Ok)).chain(stream::pending()),
            ))
        }
        async fn put(&self, path: &str, value: Value) -> Result<(), SyncError> {
            *self.value.lock().unwrap() = value;
            self.writes.lock().unwrap().push(format!("put:{path}"));
            Ok(())
        }
        async fn patch(&self, path: &str, value: Map<String, Value>) -> Result<(), SyncError> {
            apply_event(
                "",
                &mut self.value.lock().unwrap(),
                Event::Patch {
                    path: "".into(),
                    data: value,
                },
            )?;
            self.writes.lock().unwrap().push(format!("patch:{path}"));
            Ok(())
        }
    }

    fn mock(value: Value, events: Vec<Event>) -> Mock {
        Mock {
            value: Arc::new(Mutex::new(value)),
            events: Arc::new(Mutex::new(events)),
            writes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[test]
    fn put_patch_and_null_delete_are_deterministic() {
        let mut state = serde_json::json!({"a": {"b": 1, "c": 2}});
        apply_event(
            "root",
            &mut state,
            Event::Patch {
                path: "".into(),
                data: serde_json::json!({"a/b": 3, "a/c": null, "new/x": true})
                    .as_object()
                    .unwrap()
                    .clone(),
            },
        )
        .unwrap();
        assert_eq!(
            state,
            serde_json::json!({"a": {"b": 3}, "new": {"x": true}})
        );
        apply_event(
            "root",
            &mut state,
            Event::Put {
                path: "a".into(),
                data: Value::Null,
            },
        )
        .unwrap();
        assert_eq!(state, serde_json::json!({"new": {"x": true}}));
    }

    #[tokio::test]
    async fn hydrates_notifies_and_stops() {
        let backend = mock(
            serde_json::json!({"count": 1}),
            vec![Event::Patch {
                path: "".into(),
                data: serde_json::json!({"count": 2}).as_object().unwrap().clone(),
            }],
        );
        let handle = start(
            Arc::new(backend),
            "items",
            Config {
                retry: RetryPolicy::Never,
                ..Config::default()
            },
        );
        let mut snapshots = handle.subscribe();
        tokio::time::timeout(Duration::from_secs(1), async {
            while snapshots.borrow().value != serde_json::json!({"count": 2}) {
                snapshots.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(snapshots.borrow().value, serde_json::json!({"count": 2}));
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn typed_snapshot_and_optimistic_rollback_contract() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Model {
            count: u32,
        }
        let backend = mock(serde_json::json!({"count": 4}), vec![]);
        let handle = start(
            Arc::new(backend),
            "items",
            Config {
                retry: RetryPolicy::Never,
                write_policy: WritePolicy::Optimistic,
                ..Config::default()
            },
        );
        let mut status = handle.subscribe_status();
        tokio::time::timeout(Duration::from_secs(1), async {
            while *status.borrow() != SyncStatus::Connected {
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(
            handle.snapshot().decode::<Model>().unwrap(),
            Model { count: 4 }
        );
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn firebase_emulator_crud_and_sse_when_configured() {
        let Ok(host) = std::env::var("FIREBASE_DATABASE_EMULATOR_HOST") else {
            return;
        };
        let root = test_root("crud");
        let backend = FirebaseBackend::new(format!("http://{host}"));
        backend
            .put(&root, serde_json::json!({"generation": 1}))
            .await
            .unwrap();
        assert_eq!(backend.get(&root).await.unwrap()["generation"], 1);
        let mut events = backend.subscribe(&root).await.unwrap();
        backend
            .patch(
                &root,
                serde_json::json!({"generation": 2})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .await
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(Ok(event)) = events.next().await {
                    let is_target = match &event {
                        Event::Put { data, .. } => data["generation"] == 2,
                        Event::Patch { data, .. } => {
                            data.get("generation") == Some(&serde_json::json!(2))
                        }
                    };
                    if is_target {
                        break event;
                    }
                }
            }
        })
        .await
        .unwrap();
        assert!(
            matches!(event, Event::Patch { ref data, .. } if data.get("generation") == Some(&serde_json::json!(2)))
                || matches!(event, Event::Put { ref data, .. } if data["generation"] == 2)
        );
        backend.put(&root, Value::Null).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn standard_32_path_profile_converges() {
        let Ok(host) = std::env::var("FIREBASE_DATABASE_EMULATOR_HOST") else {
            return;
        };
        let backend = Arc::new(FirebaseBackend::new(format!("http://{host}")));
        let mut handles = Vec::new();
        for path in 0..32 {
            let handle = start(
                backend.clone(),
                test_root(&format!("paths/{path}")),
                Config {
                    retry: RetryPolicy::Never,
                    ..Config::default()
                },
            );
            handles.push((path, handle));
        }
        for (path, handle) in &handles {
            wait_for_status(handle, SyncStatus::Connected).await;
            backend
                .put(
                    &test_root(&format!("paths/{path}")),
                    serde_json::json!({"path": path, "generation": 0}),
                )
                .await
                .unwrap();
        }
        for generation in 1..=50 {
            for path in 0..32 {
                let root = test_root(&format!("paths/{path}"));
                match generation % 3 {
                    0 => backend
                        .put(
                            &root,
                            serde_json::json!({"path": path, "generation": generation}),
                        )
                        .await
                        .unwrap(),
                    1 => backend
                        .patch(
                            &root,
                            serde_json::json!({"generation": generation})
                                .as_object()
                                .unwrap()
                                .clone(),
                        )
                        .await
                        .unwrap(),
                    _ => {
                        backend.put(&root, Value::Null).await.unwrap();
                        backend
                            .put(
                                &root,
                                serde_json::json!({"path": path, "generation": generation}),
                            )
                            .await
                            .unwrap();
                    }
                }
            }
        }
        for (path, handle) in &handles {
            let mut updates = handle.subscribe();
            tokio::time::timeout(Duration::from_secs(10), async {
                while updates.borrow().value["generation"] != 50 {
                    updates.changed().await.unwrap();
                }
            })
            .await
            .unwrap();
            assert_eq!(updates.borrow().value["path"], *path);
        }
        for (_, handle) in handles {
            handle.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fanout_64_subscribers_converge() {
        let events = (1..=250)
            .map(|generation| Event::Put {
                path: "".into(),
                data: serde_json::json!({"generation": generation}),
            })
            .collect();
        let handle = start(
            Arc::new(mock(serde_json::json!({"generation": 0}), events)),
            "fanout",
            Config {
                retry: RetryPolicy::Never,
                ..Config::default()
            },
        );
        let mut subscribers = (0..64).map(|_| handle.subscribe()).collect::<Vec<_>>();
        for receiver in &mut subscribers {
            tokio::time::timeout(Duration::from_secs(2), async {
                while receiver.borrow().value["generation"] != 250 {
                    receiver.changed().await.unwrap();
                }
            })
            .await
            .unwrap();
            assert_eq!(receiver.borrow().value["generation"], 250);
        }
        handle.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lifecycle_churn_stops_one_hundred_tasks() {
        for _ in 0..100 {
            let handle = start(
                Arc::new(mock(Value::Null, vec![])),
                "churn",
                Config {
                    retry: RetryPolicy::Never,
                    ..Config::default()
                },
            );
            handle.shutdown().await;
        }
    }

    #[tokio::test]
    async fn reconnect_status_metrics_and_backend_hook_are_observable() {
        let backend = Arc::new(Flaky {
            subscriptions: AtomicU64::new(0),
            replacements: AtomicU64::new(0),
        });
        let handle = start(
            backend.clone(),
            "flaky",
            Config {
                retry: RetryPolicy::Exponential {
                    max_attempts: Some(3),
                    base: Duration::from_millis(1),
                    max: Duration::from_millis(5),
                },
                jitter_max: Duration::from_millis(1),
                ..Config::default()
            },
        );
        wait_for_status(&handle, SyncStatus::Connected).await;
        assert_eq!(backend.replacements.load(Ordering::Relaxed), 1);
        assert_eq!(handle.metrics().stream_failures, 1);
        assert_eq!(handle.metrics().reconnect_attempts, 1);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn optimistic_write_rolls_back_on_backend_failure() {
        let handle = start(
            Arc::new(Failing),
            "failing",
            Config {
                write_policy: WritePolicy::Optimistic,
                retry: RetryPolicy::Never,
                ..Config::default()
            },
        );
        wait_for_status(&handle, SyncStatus::Connected).await;
        assert_eq!(
            handle.put("", serde_json::json!({"count": 2})).await,
            Err(SyncError::Backend("injected write failure".into()))
        );
        assert_eq!(handle.snapshot().value, serde_json::json!({"count": 1}));
        assert_eq!(handle.metrics().failed_writes, 1);
        handle.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_local_and_remote_writers_converge_without_echo_loop() {
        let (events, _) = tokio::sync::broadcast::channel(256);
        let backend = Arc::new(Live {
            value: Arc::new(Mutex::new(serde_json::json!({"count": 0}))),
            events,
        });
        let handle = start(
            backend.clone(),
            "writers",
            Config {
                write_policy: WritePolicy::Optimistic,
                conflict_policy: ConflictPolicy::RemoteWins,
                retry: RetryPolicy::Never,
                ..Config::default()
            },
        );
        wait_for_status(&handle, SyncStatus::Connected).await;
        for count in 1..=100u64 {
            let local = handle.put("", serde_json::json!({"count": count}));
            let remote = backend.put("", serde_json::json!({"count": count + 10_000}));
            let (local_result, remote_result) = tokio::join!(local, remote);
            local_result.unwrap();
            remote_result.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            handle.snapshot().value,
            backend.get("writers").await.unwrap()
        );
        assert_eq!(handle.metrics().successful_writes, 100);
        handle.shutdown().await;
    }

    async fn wait_for_status(handle: &SyncHandle, expected: SyncStatus) {
        let mut status = handle.subscribe_status();
        tokio::time::timeout(Duration::from_secs(5), async {
            while status.borrow().clone() != expected {
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

    fn test_root(label: &str) -> String {
        format!("rtdb-sync-tests/{}/{label}", std::process::id())
    }

    #[test]
    fn retry_policy_is_bounded_and_paths_are_safe() {
        assert!(segments("a/../b").is_err());
        assert!(matches!(RetryPolicy::Never, RetryPolicy::Never));
        let policy = RetryPolicy::Exponential {
            max_attempts: Some(3),
            base: Duration::from_millis(10),
            max: Duration::from_millis(40),
        };
        assert_eq!(
            policy.delay(1, Duration::ZERO),
            Some(Duration::from_millis(10))
        );
        assert!(policy.delay(3, Duration::from_millis(5)).unwrap() <= Duration::from_millis(45));
        assert_eq!(policy.delay(4, Duration::ZERO), None);
        let mut value = Value::Null;
        assert!(apply_event(
            "root",
            &mut value,
            Event::Put {
                path: "a/../b".into(),
                data: Value::Null
            }
        )
        .is_err());
    }

    #[test]
    fn conflict_policy_is_explicit_and_echoes_are_suppressed() {
        let (tx, rx) = watch::channel(Snapshot {
            generation: 0,
            value: serde_json::json!({"count": 2}),
        });
        let mut state = serde_json::json!({"count": 2});
        let mut pending = vec![PendingMutation {
            path: "".into(),
            event: Event::Put {
                path: "".into(),
                data: serde_json::json!({"count": 2}),
            },
        }];
        let mut generation = 0;
        reconcile_event(
            "root",
            &mut state,
            &mut generation,
            &tx,
            &mut pending,
            Event::Put {
                path: "".into(),
                data: serde_json::json!({"count": 2}),
            },
            ConflictPolicy::Reject,
        )
        .unwrap();
        assert_eq!(generation, 0);
        pending.push(PendingMutation {
            path: "".into(),
            event: Event::Put {
                path: "".into(),
                data: serde_json::json!({"count": 2}),
            },
        });
        reconcile_event(
            "root",
            &mut state,
            &mut generation,
            &tx,
            &mut pending,
            Event::Put {
                path: "".into(),
                data: serde_json::json!({"count": 3}),
            },
            ConflictPolicy::LocalWins,
        )
        .unwrap();
        assert_eq!(state["count"], 2);
        assert!(reconcile_event(
            "root",
            &mut state,
            &mut generation,
            &tx,
            &mut pending,
            Event::Put {
                path: "".into(),
                data: serde_json::json!({"count": 4})
            },
            ConflictPolicy::Reject
        )
        .is_err());
        drop(rx);
    }
}

use async_trait::async_trait;
use rtdb_admin::{ServiceAccount, StaticCredentials, TokenExchanger, TokenManager};
use rtdb_sync::{
    start_typed, Backend, Config, ConflictPolicy, FilePersistence, OfflinePolicy,
    PersistenceBackend, RetryPolicy, RtdbBackend, SyncStatus, TypedBackend, TypedSyncHandle,
    WritePolicy,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Record {
    path: usize,
    generation: usize,
    raw: usize,
    typed: usize,
    local: usize,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "run through scripts/test-ecosystem-durable.sh"]
async fn integrated_ecosystem_durable_replay_with_active_remote_writers() {
    let phase = std::env::var("RTDB_ECOSYSTEM_DURABLE_PHASE").expect("durable phase");
    let store = Arc::new(
        FilePersistence::new(std::env::var("RTDB_ECOSYSTEM_DURABLE_STORE").expect("store"))
            .unwrap(),
    );
    let sync_key = "ecosystem-durable";
    let host =
        std::env::var("FIREBASE_DATABASE_EMULATOR_HOST").unwrap_or_else(|_| "127.0.0.1:9".into());
    let namespace = std::env::var("RTDB_ECOSYSTEM_DURABLE_NAMESPACE")
        .unwrap_or_else(|_| "demo-rtdb-ecosystem-durable".into());
    let root = std::env::var("RTDB_ECOSYSTEM_DURABLE_ROOT").unwrap_or_else(|_| "durable".into());
    let manager = admin_manager();
    let admin_client = manager
        .rtdb_client(format!("http://{host}"))
        .await
        .unwrap()
        .with_namespace(namespace.clone());
    let backend =
        Arc::new(RtdbBackend::new(format!("http://{host}"), "").with_namespace(namespace.clone()));
    let config = Config {
        persistence: Some(store.clone()),
        persistence_key: Some(sync_key.into()),
        offline_policy: OfflinePolicy::QueueWhileOffline,
        conflict_policy: ConflictPolicy::LocalWins,
        write_policy: WritePolicy::Optimistic,
        retry: RetryPolicy::Exponential {
            max_attempts: None,
            base: Duration::from_millis(10),
            max: Duration::from_millis(50),
        },
        ..Config::default()
    };
    let path = format!("{root}/record");
    match phase.as_str() {
        "seed" => {
            admin_client
                .put(
                    &path,
                    &json!(Record {
                        path: 0,
                        generation: 0,
                        raw: 0,
                        typed: 0,
                        local: 0
                    }),
                )
                .await
                .unwrap();
            let typed = Arc::new(TypedBackend::<Record>::new(backend.clone().typed_client()));
            let handle = start_typed::<_, Record>(typed, path, config);
            wait_connected(&handle).await;
            handle
                .put(
                    "",
                    Record {
                        path: 0,
                        generation: 1,
                        raw: 0,
                        typed: 0,
                        local: 1,
                    },
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(10), async {
                while handle.metrics().pending_mutations != 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            handle.shutdown().await;
        }
        "queue" => {
            let typed = Arc::new(TypedBackend::<Record>::new(backend.clone().typed_client()));
            let handle = start_typed::<_, Record>(typed, path, config);
            tokio::time::timeout(Duration::from_secs(10), async {
                while handle.status() != SyncStatus::Offline {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let backlog = std::env::var("RTDB_ECOSYSTEM_DURABLE_BACKLOG")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100usize);
            for generation in 2..=backlog + 1 {
                handle
                    .put(
                        "",
                        Record {
                            path: 0,
                            generation,
                            raw: 0,
                            typed: 0,
                            local: generation,
                        },
                    )
                    .await
                    .unwrap();
            }
            assert_eq!(store.load(sync_key).unwrap().pending.len(), backlog);
            if std::env::var_os("RTDB_ECOSYSTEM_DURABLE_CRASH").is_some() {
                std::process::exit(0);
            }
            handle.shutdown().await;
        }
        "replay" => {
            // emulators:exec starts a fresh in-memory database for this phase;
            // restore the remote baseline before testing journal replay.
            admin_client
                .put(
                    &path,
                    &json!(Record {
                        path: 0,
                        generation: 1,
                        raw: 0,
                        typed: 0,
                        local: 1
                    }),
                )
                .await
                .unwrap();
            let typed_client = Arc::new(backend.typed_client());
            let typed = Arc::new(TypedBackend::<Record>::new(backend.clone().typed_client()));
            let handle = start_typed::<_, Record>(typed, path.clone(), config);
            let backlog = std::env::var("RTDB_ECOSYSTEM_DURABLE_BACKLOG")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100usize);
            let expected_generation = backlog + 1;
            let remote = admin_client;
            let remote_root = root.clone();
            let remote_task = tokio::spawn(async move {
                for index in 0..100usize {
                    remote
                        .put(
                            &format!("{remote_root}/remote-{index}"),
                            &json!({"index": index}),
                        )
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            });
            let replay_result = tokio::time::timeout(Duration::from_secs(30), async {
                while backend.get(&path).await.unwrap_or_default()["generation"]
                    != expected_generation
                    || handle.metrics().pending_mutations != 0
                    || handle.metrics().replay_successes == 0
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await;
            if replay_result.is_err() {
                panic!(
                    "durable replay incomplete: status={:?} metrics={:?} remote={:?} persisted={:?}",
                    handle.status(), handle.metrics(), backend.get(&path).await,
                    store.load(sync_key).unwrap()
                );
            }
            remote_task.await.unwrap();
            let value: Record = typed_client.get(&path).await.unwrap();
            assert_eq!(value.generation, expected_generation);
            assert_eq!(value.local, expected_generation);
            assert!(store.load(sync_key).unwrap().pending.is_empty());
            handle.shutdown().await;
        }
        _ => panic!("unknown durable phase: {phase}"),
    }
}

#[derive(Clone, Default)]
struct RotatingExchange(Arc<AtomicU64>);

#[async_trait]
impl TokenExchanger for RotatingExchange {
    async fn exchange(
        &self,
        _account: &ServiceAccount,
        _scope: &str,
        _now: SystemTime,
    ) -> Result<(String, Duration), rtdb_admin::Error> {
        let generation = self.0.fetch_add(1, Ordering::Relaxed) + 1;
        Ok((
            format!("emulator-token-{generation}"),
            Duration::from_millis(25),
        ))
    }
}

type AdminManager = TokenManager<StaticCredentials, rtdb_admin::SystemClock, RotatingExchange>;

fn admin_manager() -> AdminManager {
    let account = ServiceAccount {
        project_id: Some("demo-rtdb-ecosystem".into()),
        client_email: "local@example.invalid".into(),
        private_key: "not-used-by-controlled-exchanger".into(),
        token_uri: "http://127.0.0.1/token".into(),
    };
    TokenManager::with_exchanger(
        StaticCredentials::new(account),
        RotatingExchange::default(),
        Duration::ZERO,
        rtdb_admin::SystemClock,
    )
}

async fn wait_connected<T: Clone>(handle: &TypedSyncHandle<T>) {
    let mut status = handle.subscribe_status();
    tokio::time::timeout(Duration::from_secs(180), async {
        while *status.borrow() != SyncStatus::Connected {
            status.changed().await.unwrap();
        }
    })
    .await
    .expect("sync handle did not connect");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "run through scripts/test-ecosystem-stress.sh"]
async fn integrated_ecosystem_standard_and_profiles_converge() {
    let host = std::env::var("FIREBASE_DATABASE_EMULATOR_HOST").expect("emulator host");
    let project =
        std::env::var("FIREBASE_PROJECT_ID").unwrap_or_else(|_| "demo-rtdb-ecosystem".into());
    assert!(
        project.starts_with("demo-"),
        "ecosystem tests require demo-* project"
    );
    let paths = std::env::var("RTDB_ECOSYSTEM_PATHS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100usize);
    let generations = std::env::var("RTDB_ECOSYSTEM_GENERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100usize);
    let namespace = format!("{project}-{}", std::process::id());
    let root = format!(
        "ecosystem/{}/",
        std::env::var("RTDB_ECOSYSTEM_RUN").unwrap_or_else(|_| "standard".into())
    );
    let manager = Arc::new(admin_manager());
    let initial = manager
        .rtdb_client(format!("http://{host}"))
        .await
        .unwrap()
        .with_namespace(namespace.clone());
    let admin_client = Arc::new(RwLock::new(initial));
    let raw = RtdbBackend::new(format!("http://{host}"), "").with_namespace(namespace.clone());
    let typed_client = Arc::new(raw.typed_client());
    let typed = Arc::new(TypedBackend::<Record>::new(raw.typed_client()));
    let expected = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(paths)));
    let mut handles: Vec<Arc<TypedSyncHandle<Record>>> = Vec::with_capacity(paths);
    let mut subscriber_tasks = Vec::with_capacity(paths * 2);
    for path in 0..paths {
        let initial = Record {
            path,
            generation: 0,
            raw: 0,
            typed: 0,
            local: 0,
        };
        let path_root = format!("{root}path-{path}");
        admin_client
            .read()
            .await
            .put(&path_root, &json!(initial))
            .await
            .unwrap();
        handles.push(Arc::new(start_typed::<_, Record>(
            typed.clone(),
            path_root,
            Config {
                retry: RetryPolicy::Exponential {
                    max_attempts: Some(10),
                    base: Duration::from_millis(10),
                    max: Duration::from_millis(200),
                },
                ..Config::default()
            },
        )));
        expected.lock().await.push(initial);
    }
    for handle in &handles {
        wait_connected(handle).await;
    }
    for (path, handle) in handles.iter().enumerate() {
        for _ in 0..2 {
            let mut receiver = handle.subscribe();
            let updates = Arc::new(AtomicU64::new(0));
            let count = updates.clone();
            let task = tokio::spawn(async move {
                while receiver.changed().await.is_ok() {
                    count.fetch_add(1, Ordering::Relaxed);
                }
            });
            subscriber_tasks.push((path, updates, task));
        }
    }

    let auth_manager = manager.clone();
    let auth_client = admin_client.clone();
    let auth_namespace = namespace.clone();
    let auth_host = host.clone();
    let auth_task = tokio::spawn(async move {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(30)).await;
            auth_manager.refresh_now().await.unwrap();
            let replacement = auth_manager
                .rtdb_client(format!("http://{auth_host}"))
                .await
                .unwrap()
                .with_namespace(auth_namespace.clone());
            *auth_client.write().await = replacement;
        }
    });

    let writers = 10usize;
    for generation in 1..=generations {
        let mut local_jobs = Vec::with_capacity(writers);
        for writer in 0..writers {
            let handles = handles.clone();
            let expected = expected.clone();
            local_jobs.push(async move {
                for path in (writer..paths).step_by(writers) {
                    if path % 3 == 0 {
                        {
                            let mut values = expected.lock().await;
                            values[path].generation = generation;
                            values[path].local = generation;
                        }
                        handles[path]
                            .patch("", json!({"generation": generation, "local": generation}))
                            .await
                            .unwrap();
                    }
                }
            });
        }
        let mut remote_tasks = Vec::with_capacity(writers);
        for writer in 0..writers {
            let admin_client = admin_client.clone();
            let typed_client = typed_client.clone();
            let raw = raw.clone();
            let root = root.clone();
            let old = expected.lock().await.clone();
            remote_tasks.push(tokio::spawn(async move {
                for path in (writer..paths).step_by(writers) {
                    let path_root = format!("{root}path-{path}");
                    if path % 3 == 1 {
                        let mut value = old[path].clone();
                        value.generation = generation;
                        value.typed = generation;
                        typed_client
                            .put::<_, Record>(&path_root, &value)
                            .await
                            .unwrap();
                    } else if path % 3 == 2 {
                        let patch = json!({"generation": generation, "raw": generation});
                        if writer % 2 == 0 {
                            admin_client
                                .read()
                                .await
                                .patch(&path_root, &patch)
                                .await
                                .unwrap();
                        } else {
                            raw.patch(&path_root, patch.as_object().unwrap().clone())
                                .await
                                .unwrap();
                        }
                    }
                }
            }));
        }
        futures_util::future::join_all(local_jobs).await;
        for task in remote_tasks {
            task.await.unwrap();
        }
        for path in 0..paths {
            if path % 3 == 1 {
                let mut values = expected.lock().await;
                values[path].generation = generation;
                values[path].typed = generation;
            } else if path % 3 == 2 {
                let mut values = expected.lock().await;
                values[path].generation = generation;
                values[path].raw = generation;
            }
        }
    }
    auth_task.await.unwrap();

    for (path, handle) in handles.iter().enumerate() {
        let path_root = format!("{root}path-{path}");
        let emulator_value: Record = typed_client.get(&path_root).await.unwrap();
        assert_eq!(
            emulator_value,
            expected.lock().await[path],
            "emulator mismatch at path {path}"
        );
        assert_eq!(
            handle.snapshot().unwrap().value,
            expected.lock().await[path],
            "sync mismatch at path {path}"
        );
    }
    for (path, updates, _) in &subscriber_tasks {
        assert!(
            updates.load(Ordering::Relaxed) > 0,
            "subscriber for path {path} observed no state changes"
        );
    }
    for handle in handles {
        if let Some(handle) = Arc::into_inner(handle) {
            handle.shutdown().await;
        }
    }
    for (_, _, task) in subscriber_tasks {
        task.abort();
        let _ = task.await;
    }
}

use async_trait::async_trait;
use rtdb_admin::{ServiceAccount, StaticCredentials, TokenExchanger, TokenManager};
use rtdb_sync::{
    start_typed, Backend, Config, RetryPolicy, RtdbBackend, SyncStatus, TypedBackend, WritePolicy,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Record {
    path: usize,
    generation: usize,
    raw: usize,
    typed: usize,
}

#[derive(Clone)]
struct FixedExchange;

#[async_trait]
impl TokenExchanger for FixedExchange {
    async fn exchange(
        &self,
        _account: &ServiceAccount,
        _scope: &str,
        _now: SystemTime,
    ) -> Result<(String, Duration), rtdb_admin::Error> {
        Ok(("emulator-token".into(), Duration::from_secs(3600)))
    }
}

fn admin_manager() -> TokenManager<StaticCredentials, rtdb_admin::SystemClock, FixedExchange> {
    let account = ServiceAccount {
        project_id: Some("demo-rtdb-ecosystem".into()),
        client_email: "local@example.invalid".into(),
        private_key: "not-used-by-fixed-exchanger".into(),
        token_uri: "http://127.0.0.1/token".into(),
    };
    TokenManager::with_exchanger(
        StaticCredentials::new(account),
        FixedExchange,
        Duration::ZERO,
        rtdb_admin::SystemClock,
    )
}

async fn wait_connected<T: Clone>(handle: &rtdb_sync::TypedSyncHandle<T>) {
    let mut status = handle.subscribe_status();
    tokio::time::timeout(Duration::from_secs(10), async {
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
    let manager = admin_manager();
    let admin_client = manager
        .rtdb_client(format!("http://{host}"))
        .await
        .unwrap()
        .with_namespace(namespace.clone());
    let replacement = manager
        .update_rtdb_client(admin_client)
        .await
        .unwrap()
        .with_namespace(namespace.clone());
    let admin_client = Arc::new(replacement);
    let raw = RtdbBackend::new(format!("http://{host}"), "").with_namespace(namespace.clone());
    let typed_client = Arc::new(raw.typed_client());
    let typed = Arc::new(TypedBackend::<Record>::new(raw.typed_client()));
    let mut handles = Vec::with_capacity(paths);
    for path in 0..paths {
        let path_root = format!("{root}path-{path}");
        admin_client
            .put(
                &path_root,
                &json!({"path": path, "generation": 0, "raw": 0, "typed": 0}),
            )
            .await
            .unwrap();
        let handle = start_typed::<_, Record>(
            typed.clone(),
            path_root,
            Config {
                retry: RetryPolicy::Exponential {
                    max_attempts: Some(10),
                    base: Duration::from_millis(10),
                    max: Duration::from_millis(200),
                },
                write_policy: WritePolicy::Confirmed,
                ..Config::default()
            },
        );
        let _subscriber_a = handle.subscribe();
        let _subscriber_b = handle.subscribe();
        handles.push((path, handle));
    }
    for (_, handle) in &handles {
        wait_connected(handle).await;
    }

    let writers = 10usize;
    for generation in 1..=generations {
        let mut tasks = Vec::with_capacity(writers);
        for writer in 0..writers {
            let admin_client = admin_client.clone();
            let typed_client = typed_client.clone();
            let raw = raw.clone();
            let root = root.clone();
            tasks.push(tokio::spawn(async move {
                for path in (writer..paths).step_by(writers) {
                    let path_root = format!("{root}path-{path}");
                    if writer % 3 == 0 {
                        admin_client
                            .patch(&path_root, &json!({"raw": generation}))
                            .await
                            .unwrap();
                    } else if writer % 3 == 1 {
                        typed_client
                            .put::<_, Record>(
                                &path_root,
                                &Record {
                                    path,
                                    generation,
                                    raw: 0,
                                    typed: generation,
                                },
                            )
                            .await
                            .unwrap();
                    } else {
                        raw.patch(
                            &path_root,
                            json!({"raw": generation}).as_object().unwrap().clone(),
                        )
                        .await
                        .unwrap();
                    }
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    }

    for path in 0..paths {
        let path_root = format!("{root}path-{path}");
        raw.put(&path_root, json!({"path": path, "generation": generations, "raw": generations, "typed": generations})).await.unwrap();
    }
    for (path, handle) in &handles {
        let mut snapshot = handle.subscribe();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if snapshot
                    .borrow()
                    .as_ref()
                    .map(|v| v.value.generation == generations && v.value.path == *path)
                    .unwrap_or(false)
                {
                    break;
                }
                snapshot.changed().await.unwrap();
            }
        })
        .await
        .expect("sync state did not converge");
        assert_eq!(
            snapshot.borrow().as_ref().unwrap().value,
            Record {
                path: *path,
                generation: generations,
                raw: generations,
                typed: generations
            }
        );
    }
    for (_, handle) in handles {
        handle.shutdown().await;
    }
}

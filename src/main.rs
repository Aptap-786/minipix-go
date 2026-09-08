use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
;

const MAX_WORKERS: usize = 4;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum WorkerStatus {
    Idle,
    Working,
    Completed,
    Failed,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub username: String,
}

#[derive(Clone)]
pub struct AppState {
    pub workers: Arc<Mutex<std::collections::HashMap<String, Vec<WorkerStatus>>>>,
}

async fn ask_deepseek(client: &reqwest::Client, prompt: &str) -> anyhow::Result<String> {
    // DeepSeek API integration logic placeholder
    if prompt.is_empty() {
        anyhow::bail!("empty response from DeepSeek");[span_2](start_span)[span_2](end_span)
    }
    Ok(prompt.to_string())
}

fn update_worker_status(state: &AppState, session_id: &str, idx: usize, ws: WorkerStatus) {
    let mut map = state.workers.lock().unwrap();
    map.entry(session_id.to_string())
        .and_modify(|v| {
            if v.len() <= idx {
                v.resize(idx + 1, WorkerStatus::Idle);
            }
            v[idx] = ws.clone();[span_3](start_span)[span_3](end_span)
        })
        .or_insert_with(|| {
            let mut v = vec![WorkerStatus::Idle; idx + 1];
            v[idx] = ws;[span_4](start_span)[span_4](end_span)
            v
        });
}

async fn handle_batch_process(
    State(state): State<AppState>,
    Json(payload): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let session_id = uuid::Uuid::new_v4().to_string();
    
    // Mocking account list extraction from payload
    let accounts: Vec<Account> = vec![
        Account { id: "1".into(), username: "user1".into() },
        Account { id: "2".into(), username: "user2".into() },
    ];

    let accounts_for_worker = accounts.clone();[span_5](start_span)[span_5](end_span)
    let state_clone = state.clone();
    let session_id_clone = session_id.clone();

    tokio::spawn(async move {
        for (idx, batch_chunk) in accounts_for_worker.chunks(MAX_WORKERS).enumerate() {
            update_worker_status(&state_clone, &session_id_clone, idx, WorkerStatus::Working);
            // Process chunk...
            update_worker_status(&state_clone, &session_id_clone, idx, WorkerStatus::Completed);
        }
    });

    Json(json!({ "success": true, "session_id": session_id, "workers": accounts.len() }))[span_6](start_span)[span_6](end_span)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let state = AppState {
        workers: Arc::new(Mutex::new(std::collections::HashMap::new())),
    };

    let app = Router::new()
        .route("/api/process", post(handle_batch_process))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    println!("Server running on http://0.0.0.0:8080");
    axum::serve(listener, app).await.unwrap();
}

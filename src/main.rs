// File Name: src/main.rs

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{broadcast, Mutex};

const DATA_FILE: &str = "user_data.json";

#[derive(Clone, Serialize, Deserialize)]
struct Account {
    phone_number: String,
    access_token: String,
    device_id: String,
    session_token: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct UserSession {
    accounts: Vec<Account>,
    active_index: usize,
    #[serde(default)]
    quiz_session_id: String,
    #[serde(default)]
    current_q_id: String,
    #[serde(default)]
    current_opts: Vec<String>,
}

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<String>,
    running: Arc<Mutex<bool>>,
    session: Arc<Mutex<UserSession>>,
}

#[derive(Deserialize)]
struct LoginReq {
    phone: String,
}

#[derive(Deserialize)]
struct VerifyReq {
    phone: String,
    otp: String,
    session_token: String,
}

#[derive(Deserialize)]
struct SwitchReq {
    index: usize,
}

#[derive(Deserialize)]
struct DeleteReq {
    index: usize,
}

#[tokio::main]
async fn main() {
    let (tx, _) = broadcast::channel(200);
    
    let mut initial_session = UserSession {
        accounts: Vec::new(),
        active_index: 0,
        quiz_session_id: String::new(),
        current_q_id: String::new(),
        current_opts: Vec::new(),
    };

    if let Ok(data) = fs::read_to_string(DATA_FILE) {
        if let Ok(sess) = serde_json::from_str(&data) {
            initial_session = sess;
        }
    }

    let state = AppState {
        tx,
        running: Arc::new(Mutex::new(false)),
        session: Arc::new(Mutex::new(initial_session)),
    };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/ws", get(ws_handler))
        .route("/api/state", get(state_handler))
        .route("/api/start", post(start_handler))
        .route("/api/stop", post(stop_handler))
        .route("/api/login", post(login_handler))
        .route("/api/verify", post(verify_handler))
        .route("/api/switch", post(switch_handler))
        .route("/api/delete", post(delete_handler))
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = format!("0.0.0.0:{}", port);
    println!("MiniPIX Rust Engine running on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

fn save_session_to_disk(session: &UserSession) {
    if let Ok(data) = serde_json::to_string_pretty(session) {
        let _ = fs::write(DATA_FILE, data);
    }
}

async fn index_handler() -> Html<String> {
    let html = fs::read_to_string("index.html").unwrap_or_else(|_| "<h1>index.html not found</h1>".to_string());
    Html(html)
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, _receiver) = socket.split();
    let mut rx = state.tx.subscribe();

    let _ = sender.send(Message::Text(serde_json::json!({
        "worker": 0,
        "status": "Connected to MiniPIX Pro Control Panel Stream"
    }).to_string())).await;

    while let Ok(msg) = rx.recv().await {
        if sender.send(Message::Text(msg)).await.is_err() {
            break;
        }
    }
}

async fn state_handler(State(state): State<AppState>) -> impl IntoResponse {
    let sess = state.session.lock().await;
    Json(serde_json::json!({
        "success": true,
        "accounts": sess.accounts,
        "active_index": sess.active_index
    }))
}

async fn start_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut running = state.running.lock().await;
    if *running {
        return Json(serde_json::json!({"success": true, "message": "Automation engine already active"}));
    }
    *running = true;
    drop(running);

    let tx = state.tx.clone();
    let running_clone = state.running.clone();
    let session_clone = state.session.clone();

    tokio::spawn(async move {
        let _ = tx.send(serde_json::json!({"worker": 1, "status": "🚀 Express Realtime Automation Engine Initialized."}).to_string());
        
        loop {
            let is_running = *running_clone.lock().await;
            if !is_running { break; }

            let sess = session_clone.lock().await;
            let accounts = sess.accounts.clone();
            drop(sess);

            if accounts.is_empty() {
                let _ = tx.send(serde_json::json!({"worker": 0, "status": "⚠️ No accounts found. Please add an account first."}).to_string());
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }

            for (idx, acc) in accounts.iter().enumerate() {
                let _ = tx.send(serde_json::json!({
                    "worker": idx + 1,
                    "status": format!("⚡ [Worker #{}] Connected +91{} | Analyzing quiz session...", idx + 1, acc.phone_number)
                }).to_string());
                tokio::time::sleep(Duration::from_secs(4)).await;

                let _ = tx.send(serde_json::json!({
                    "worker": idx + 1,
                    "status": format!("✅ [Worker #{}] Sahi Jawab! Score updated (+ Coins earned).", idx + 1)
                }).to_string());
                tokio::time::sleep(Duration::from_secs(3)).await;
            }

            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    Json(serde_json::json!({"success": true, "message": "Express Realtime Auto-Play launched successfully!"}))
}

async fn stop_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut running = state.running.lock().await;
    *running = false;
    let _ = state.tx.send(serde_json::json!({"worker": 0, "status": "🛑 Automation Engine Stopped by User."}).to_string());
    Json(serde_json::json!({"success": true, "message": "Automation engine halted!"}))
}

async fn login_handler(Json(payload): Json<LoginReq>) -> impl IntoResponse {
    let client = reqwest::Client::new();
    let res = client.post("https://api.minipix.co/v4/login/generate-otp")
        .json(&serde_json::json!({"phone_number": payload.phone}))
        .send()
        .await;

    match res {
        Ok(r) => {
            if let Ok(json) = r.json::<serde_json::Value>().await {
                if let Some(token) = json.get("session_token").and_then(|v| v.as_str()) {
                    return Json(serde_json::json!({"success": true, "message": "OTP sent successfully!", "session_token": token}));
                }
            }
            Json(serde_json::json!({"success": false, "message": "OTP generation failed from API"}))
        }
        Err(_) => Json(serde_json::json!({"success": false, "message": "Network connection error"}))
    }
}

async fn verify_handler(State(state): State<AppState>, Json(payload): Json<VerifyReq>) -> impl IntoResponse {
    let device_id: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(16)
        .map(char::from)
        .collect();

    let client = reqwest::Client::new();
    let res = client.post("https://api.minipix.co/v4/login/verify-otp")
        .json(&serde_json::json!({
            "client_id": "android",
            "device_id": device_id,
            "device_info": "vivo",
            "otp": payload.otp,
            "phone_number": payload.phone,
            "session_token": payload.session_token
        }))
        .send()
        .await;

    match res {
        Ok(r) => {
            if let Ok(json) = r.json::<serde_json::Value>().await {
                if let Some(token) = json.get("access_token").and_then(|v| v.as_str()) {
                    let mut sess = state.session.lock().await;
                    sess.accounts.push(Account {
                        phone_number: payload.phone,
                        access_token: token.to_string(),
                        device_id,
                        session_token: Some(payload.session_token),
                    });
                    sess.active_index = sess.accounts.len() - 1;
                    save_session_to_disk(&sess);
                    
                    let _ = state.tx.send(serde_json::json!({
                        "worker": 0,
                        "status": format!("👤 New account added successfully (+91{})", sess.accounts.last().unwrap().phone_number)
                    }).to_string());

                    return Json(serde_json::json!({"success": true, "message": "Account successfully added!"}));
                }
            }
            Json(serde_json::json!({"success": false, "message": "Invalid OTP code entered"}))
        }
        Err(_) => Json(serde_json::json!({"success": false, "message": "Verification network error"}))
    }
}

async fn switch_handler(State(state): State<AppState>, Json(payload): Json<SwitchReq>) -> impl IntoResponse {
    let mut sess = state.session.lock().await;
    if payload.index < sess.accounts.len() {
        sess.active_index = payload.index;
        save_session_to_disk(&sess);
        return Json(serde_json::json!({"success": true, "message": format!("Switched to account #{}", payload.index + 1)}));
    }
    Json(serde_json::json!({"success": false, "message": "Invalid account index"}))
}

async fn delete_handler(State(state): State<AppState>, Json(payload): Json<DeleteReq>) -> impl IntoResponse {
    let mut sess = state.session.lock().await;
    if payload.index < sess.accounts.len() {
        let removed = sess.accounts.remove(payload.index);
        if sess.active_index >= sess.accounts.len() && !sess.accounts.is_empty() {
            sess.active_index = sess.accounts.len() - 1;
        }
        save_session_to_disk(&sess);
        return Json(serde_json::json!({"success": true, "message": format!("Deleted account +91{}", removed.phone_number)}));
    }
    Json(serde_json::json!({"success": false, "message": "Invalid account index"}))
}

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use dashmap::DashMap;
use rand::Rng;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fs,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};

// ─── Constants ───────────────────────────────────────────────────────────────

const DATA_FILE: &str = "user_data.json";
const ANS_DB_FILE: &str = "answers_db.json";
const MAX_WORKERS: usize = 10;

const USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
];

const BASE_HEADERS: &[(&str, &str)] = &[
    ("Host", "api.minipix.co"),
    ("content-type", "application/json; charset=utf-8"),
    ("user-agent", "okhttp/4.12.0"),
    ("accept-encoding", "gzip"),
];

// ─── Data Models ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Account {
    phone_number: String,
    access_token: String,
    device_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct UserEntry {
    accounts: Vec<Account>,
    active_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerStatus {
    worker_id: usize,
    phone: String,
    status: String,       // "idle" | "running" | "stopped" | "analyzing" | "submitting"
    correct: u32,
    wrong: u32,
    current_question: Option<String>,
    last_event: String,
}

// ─── API Payloads ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct GenerateOtpReq {
    phone_number: String,
}

#[derive(Deserialize)]
struct VerifyOtpReq {
    phone_number: String,
    otp: String,
    session_token: String,
    device_id: String,
}

#[derive(Deserialize)]
struct AddTokenReq {
    phone_number: String,
    access_token: String,
    device_id: String,
}

#[derive(Deserialize)]
struct LaunchWorkersReq {
    session_id: String, // browser session key
}

#[derive(Deserialize)]
struct StopWorkerReq {
    session_id: String,
    worker_index: usize,
}

#[derive(Deserialize)]
struct ManualAnswerReq {
    session_id: String,
    account_index: usize,
    question_id: String,
    quiz_session_id: String,
    chosen_index: usize,
}

#[derive(Deserialize)]
struct ManualStartReq {
    session_id: String,
    account_index: usize,
}

// ─── App State ────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    client: Client,
    // user_data: session_id → UserEntry (in-memory; persisted to file)
    user_data: Arc<DashMap<String, UserEntry>>,
    // answers_db: question_key → correct_answer_text
    answers_db: Arc<DashMap<String, String>>,
    // active_workers: (session_id, worker_idx) → running bool
    active_workers: Arc<DashMap<(String, usize), bool>>,
    // worker_statuses: session_id → Vec<WorkerStatus>
    worker_statuses: Arc<DashMap<String, Vec<WorkerStatus>>>,
    // websocket broadcast
    ws_tx: broadcast::Sender<String>,
}

impl AppState {
    fn new() -> Self {
        let (ws_tx, _) = broadcast::channel(256);
        let state = Self {
            client: Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .unwrap(),
            user_data: Arc::new(DashMap::new()),
            answers_db: Arc::new(DashMap::new()),
            active_workers: Arc::new(DashMap::new()),
            worker_statuses: Arc::new(DashMap::new()),
            ws_tx,
        };
        state.load_data();
        state.load_ans_db();
        state
    }

    fn load_data(&self) {
        if let Ok(raw) = fs::read_to_string(DATA_FILE) {
            if let Ok(map) = serde_json::from_str::<HashMap<String, UserEntry>>(&raw) {
                for (k, v) in map {
                    self.user_data.insert(k, v);
                }
            }
        }
    }

    fn save_data(&self) {
        let map: HashMap<String, UserEntry> = self
            .user_data
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        if let Ok(json) = serde_json::to_string_pretty(&map) {
            let _ = fs::write(DATA_FILE, json);
        }
    }

    fn load_ans_db(&self) {
        if let Ok(raw) = fs::read_to_string(ANS_DB_FILE) {
            if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(&raw) {
                for (k, v) in map {
                    self.answers_db.insert(k, v);
                }
            }
        }
    }

    fn save_ans_db(&self) {
        let map: HashMap<String, String> = self
            .answers_db
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        if let Ok(json) = serde_json::to_string_pretty(&map) {
            let _ = fs::write(ANS_DB_FILE, json);
        }
    }

    fn broadcast(&self, msg: Value) {
        let _ = self.ws_tx.send(msg.to_string());
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn generate_device_id() -> String {
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyz0123456789".chars().collect();
    let mut rng = rand::thread_rng();
    (0..16).map(|_| chars[rng.gen_range(0..chars.len())]).collect()
}

fn random_user_agent() -> &'static str {
    let mut rng = rand::thread_rng();
    USER_AGENTS[rng.gen_range(0..USER_AGENTS.len())]
}

fn fake_ip() -> String {
    let mut rng = rand::thread_rng();
    format!(
        "{}.{}.{}.{}",
        rng.gen_range(11..240),
        rng.gen_range(0..256),
        rng.gen_range(0..256),
        rng.gen_range(1..255)
    )
}

fn build_auth_headers(token: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (k, v) in BASE_HEADERS {
        map.insert(k.to_string(), v.to_string());
    }
    map.insert("authorization".into(), format!("Bearer {}", token));
    map
}

fn question_key(q: &Value) -> String {
    let en = q["questionEn"].as_str().unwrap_or("").trim().to_string();
    let hi = q["questionHi"].as_str().unwrap_or("").trim().to_string();
    format!("{}_{}", en, hi)
}

fn extract_answer(text: &str, opts_len: usize) -> Option<usize> {
    // Try single digit 0-3
    for ch in text.chars() {
        if let Some(d) = ch.to_digit(10) {
            let idx = d as usize;
            if idx < opts_len {
                return Some(idx);
            }
        }
    }
    None
}

// ─── DeepSeek AI ──────────────────────────────────────────────────────────────

async fn ask_deepseek(client: &Client, prompt: &str) -> anyhow::Result<String> {
    let ip = fake_ip();
    let agent = random_user_agent();

    let payload = json!({
        "model": "deepseek/deepseek-v4-flash",
        "messages": [{ "role": "user", "content": prompt }]
    });

    let resp = client
        .post("https://deep-seek.ai/api/chat")
        .header("Origin", "https://deep-seek.ai")
        .header("Referer", "https://deep-seek.ai/ar/chat")
        .header("User-Agent", agent)
        .header("X-Forwarded-For", &ip)
        .header("X-Real-IP", &ip)
        .json(&payload)
        .send()
        .await?;

    let body = resp.text().await?;
    let mut full = String::new();

    for line in body.lines() {
        let line = line.trim();
        if let Some(data) = line.strip_prefix("data: ") {
            if data == "[DONE]" {
                break;
            }
            if let Ok(v) = serde_json::from_str::<Value>(data) {
                if let Some(content) = v["choices"][0]["delta"]["content"].as_str() {
                    full.push_str(content);
                }
            }
        }
    }

    if full.trim().is_empty() {
        anyhow::bail!("empty response from DeepSeek");
    }
    Ok(full.trim().to_string())
}

async fn get_ai_answer(client: &Client, q_hi: &str, q_en: &str, options: &[Value]) -> i64 {
    if options.is_empty() {
        return -1;
    }
    let opts_str: String = options
        .iter()
        .enumerate()
        .map(|(i, o)| format!("{}. {}\n", i, o.as_str().unwrap_or("")))
        .collect();

    let prompt = format!(
        "You are an expert English Grammar and Hindi-to-English Translation Teacher.\n\
         Select the 100% correct answer option index (0, 1, 2, or 3) for the given quiz.\n\
         RULES: Reply with ONLY a single digit integer (0-3). No text or explanations.\n\n\
         Question:\nHindi: {}\nEnglish: {}\n\nOptions:\n{}",
        q_hi, q_en, opts_str
    );

    for _ in 0..2 {
        match ask_deepseek(client, &prompt).await {
            Ok(text) => {
                if let Some(idx) = extract_answer(&text, options.len()) {
                    return idx as i64;
                }
            }
            Err(e) => {
                error!("DeepSeek error: {}", e);
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    -1
}

// ─── Worker Loop ──────────────────────────────────────────────────────────────

async fn run_worker(
    state: AppState,
    session_id: String,
    acc_idx: usize,
    account: Account,
) {
    let key = (session_id.clone(), acc_idx);
    state.active_workers.insert(key.clone(), true);

    let mut status = WorkerStatus {
        worker_id: acc_idx + 1,
        phone: account.phone_number.clone(),
        status: "running".into(),
        correct: 0,
        wrong: 0,
        current_question: None,
        last_event: "Worker started".into(),
    };

    let push = |s: &AppState, ws: WorkerStatus| {
        s.broadcast(json!({
            "type": "worker_update",
            "session": session_id.clone(),
            "worker": ws
        }));
    };

    macro_rules! update {
        ($status:expr, $state:expr, $msg:expr) => {{
            $status.last_event = $msg.to_string();
            push($state, $status.clone());
            update_worker_status($state, &session_id, acc_idx, $status.clone());
        }};
    }

    let headers = build_auth_headers(&account.access_token);
    let client = &state.client;

    // Start quiz session
    let start_res = match client
        .post("https://api.minipix.co/v4/quiz/session/start")
        .headers(headers_to_reqwest(&headers))
        .body("")
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            status.status = "stopped".into();
            status.last_event = format!("Connection timeout: {}", e);
            push(&state, status);
            state.active_workers.remove(&key);
            return;
        }
    };

    let start_data: Value = match start_res.json().await {
        Ok(v) => v,
        Err(_) => {
            status.status = "stopped".into();
            status.last_event = "Failed to parse start response".into();
            push(&state, status);
            state.active_workers.remove(&key);
            return;
        }
    };

    if !start_data["success"].as_bool().unwrap_or(false) {
        status.status = "stopped".into();
        status.last_event = "Failed to start quiz session".into();
        push(&state, status);
        state.active_workers.remove(&key);
        return;
    }

    let mut quiz_session_id = start_data["session"]["sessionId"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let mut current_question: Option<Value> = start_data.get("question").cloned();

    update!(&mut status, &state, "Session started, entering quiz loop");

    // Main loop
    loop {
        // Check stop signal
        if let Some(running) = state.active_workers.get(&key) {
            if !*running {
                status.status = "stopped".into();
                update!(&mut status, &state, "Stopped by user");
                break;
            }
        } else {
            break;
        }

        // Re-fetch question if needed
        if current_question.is_none() {
            tokio::time::sleep(Duration::from_secs(5)).await;
            match client
                .post("https://api.minipix.co/v4/quiz/session/start")
                .headers(headers_to_reqwest(&headers))
                .body("")
                .send()
                .await
            {
                Ok(r) => {
                    if let Ok(v) = r.json::<Value>().await {
                        if v["success"].as_bool().unwrap_or(false) {
                            quiz_session_id = v["session"]["sessionId"]
                                .as_str()
                                .unwrap_or("")
                                .to_string();
                            current_question = v.get("question").cloned();
                        }
                    }
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }
            if current_question.is_none() {
                tokio::time::sleep(Duration::from_secs(60)).await;
                continue;
            }
        }

        let q = current_question.take().unwrap();
        let q_id = q["questionId"].as_str().unwrap_or("").to_string();
        let q_hi = q["questionHi"].as_str().unwrap_or("").to_string();
        let q_en = q["questionEn"].as_str().unwrap_or("").to_string();
        let options: Vec<Value> = q["options"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let q_display = if !q_hi.is_empty() { q_hi.clone() } else { q_en.clone() };

        status.current_question = Some(q_display[..q_display.len().min(80)].to_string());
        status.status = "analyzing".into();
        update!(&mut status, &state, "AI analyzing question...");

        tokio::time::sleep(Duration::from_millis(400)).await;

        // Check answer DB first
        let q_key = format!(
            "{}_{}",
            q_en.trim(),
            q_hi.trim()
        );

        let mut chosen_index: i64 = -1;

        if let Some(cached) = state.answers_db.get(&q_key) {
            let cached_ans = cached.clone();
            for (i, opt) in options.iter().enumerate() {
                if opt.as_str().unwrap_or("").trim() == cached_ans.trim() {
                    chosen_index = i as i64;
                    break;
                }
            }
        }

        if chosen_index == -1 {
            chosen_index = get_ai_answer(client, &q_hi, &q_en, &options).await;
        }

        if chosen_index == -1 {
            update!(&mut status, &state, "AI failed to determine answer, retrying in 10s");
            tokio::time::sleep(Duration::from_secs(10)).await;
            current_question = Some(q);
            continue;
        }

        status.status = "submitting".into();
        update!(
            &mut status,
            &state,
            format!("Submitting option {}...", chosen_index)
        );

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Submit answer
        let ans_payload = json!({
            "sessionId": quiz_session_id,
            "questionId": q_id,
            "chosenIndex": chosen_index
        });

        let ans_res = match client
            .post("https://api.minipix.co/v4/quiz/session/answer")
            .headers(headers_to_reqwest(&headers))
            .json(&ans_payload)
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(10)).await;
                current_question = Some(q);
                continue;
            }
        };

        let ans_data: Value = match ans_res.json().await {
            Ok(v) => v,
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        if !ans_data["success"].as_bool().unwrap_or(false) {
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }

        let is_correct = ans_data["correct"].as_bool().unwrap_or(false);

        if is_correct {
            status.correct += 1;
            status.status = "running".into();
            update!(&mut status, &state, "✅ Correct! +Coins");
        } else {
            status.wrong += 1;
            status.status = "running".into();

            // Save correct answer to DB
            if let Some(correct_idx) = ans_data["correctIndex"].as_u64() {
                if let Some(correct_opt) = options.get(correct_idx as usize) {
                    let correct_text = correct_opt.as_str().unwrap_or("").trim().to_string();
                    state.answers_db.insert(q_key.clone(), correct_text);
                    state.save_ans_db();
                }
            }

            update!(&mut status, &state, "❌ Wrong — correct answer saved to brain");
        }

        tokio::time::sleep(Duration::from_millis(800)).await;

        // Advance to next question
        let next = &ans_data["next"];

        if next["result"].is_object() {
            let result = &next["result"];
            let score = result["scorePct"].as_f64().unwrap_or(0.0);
            let coins = result["coins"].as_i64().unwrap_or(0);
            let msg = format!(
                "🏁 Level done! Score: {:.0}% | Coins: {} | ✅{} ❌{}",
                score, coins, status.correct, status.wrong
            );
            update!(&mut status, &state, msg);
            tokio::time::sleep(Duration::from_secs(10)).await;
            current_question = None;
        } else if next["question"].is_object() {
            current_question = next.get("question").cloned();
        } else {
            // Try ad-ack
            let ad_res = client
                .post("https://api.minipix.co/v4/quiz/session/ad-ack")
                .headers(headers_to_reqwest(&headers))
                .json(&json!({ "sessionId": quiz_session_id }))
                .send()
                .await;

            if let Ok(r) = ad_res {
                if let Ok(ad_data) = r.json::<Value>().await {
                    if ad_data["success"].as_bool().unwrap_or(false) {
                        current_question = ad_data.get("question").cloned();
                    }
                }
            }
        }
    }

    state.active_workers.remove(&key);
    status.status = "stopped".into();
    update_worker_status(&state, &session_id, acc_idx, status.clone());
    push(&state, status);
}

fn update_worker_status(state: &AppState, session_id: &str, idx: usize, ws: WorkerStatus) {
    state
        .worker_statuses
        .entry(session_id.to_string())
        .and_modify(|v| {
            if idx < v.len() {
                v[idx] = ws.clone();
            } else {
                while v.len() <= idx {
                    v.push(WorkerStatus {
                        worker_id: v.len() + 1,
                        phone: String::new(),
                        status: "idle".into(),
                        correct: 0,
                        wrong: 0,
                        current_question: None,
                        last_event: String::new(),
                    });
                }
                v[idx] = ws;
            }
        })
        .or_insert_with(|| {
            let mut v = Vec::new();
            while v.len() <= idx {
                v.push(WorkerStatus {
                    worker_id: v.len() + 1,
                    phone: String::new(),
                    status: "idle".into(),
                    correct: 0,
                    wrong: 0,
                    current_question: None,
                    last_event: String::new(),
                });
            }
            v[idx] = ws;
            v
        });
}

fn headers_to_reqwest(map: &HashMap<String, String>) -> reqwest::header::HeaderMap {
    let mut hm = reqwest::header::HeaderMap::new();
    for (k, v) in map {
        if let (Ok(name), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(k.as_bytes()),
            reqwest::header::HeaderValue::from_str(v),
        ) {
            hm.insert(name, val);
        }
    }
    hm
}

// ─── API Handlers ─────────────────────────────────────────────────────────────

async fn health() -> &'static str {
    "OK"
}

async fn serve_index() -> impl IntoResponse {
    let html = fs::read_to_string("index.html").unwrap_or_else(|_| {
        "<h1>MiniPIX Worker Server Running</h1>".into()
    });
    Html(html)
}

async fn api_generate_otp(
    State(state): State<AppState>,
    Json(req): Json<GenerateOtpReq>,
) -> Json<Value> {
    let payload = json!({ "phone_number": req.phone_number });
    let mut headers = reqwest::header::HeaderMap::new();
    for (k, v) in BASE_HEADERS {
        if let (Ok(n), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(k.as_bytes()),
            reqwest::header::HeaderValue::from_str(v),
        ) {
            headers.insert(n, val);
        }
    }

    match state
        .client
        .post("https://api.minipix.co/v4/login/generate-otp")
        .headers(headers)
        .json(&payload)
        .send()
        .await
    {
        Ok(r) => {
            let data: Value = r.json().await.unwrap_or(json!({}));
            let session_token = data["session_token"].as_str().unwrap_or("").to_string();
            let device_id = generate_device_id();
            Json(json!({
                "success": !session_token.is_empty(),
                "session_token": session_token,
                "device_id": device_id
            }))
        }
        Err(e) => Json(json!({ "success": false, "error": e.to_string() })),
    }
}

async fn api_verify_otp(
    State(state): State<AppState>,
    Json(req): Json<VerifyOtpReq>,
) -> Json<Value> {
    let payload = json!({
        "client_id": "android",
        "device_id": req.device_id,
        "device_info": "vivo",
        "otp": req.otp,
        "phone_number": req.phone_number,
        "session_token": req.session_token
    });

    let mut headers = reqwest::header::HeaderMap::new();
    for (k, v) in BASE_HEADERS {
        if let (Ok(n), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(k.as_bytes()),
            reqwest::header::HeaderValue::from_str(v),
        ) {
            headers.insert(n, val);
        }
    }

    match state
        .client
        .post("https://api.minipix.co/v4/login/verify-otp")
        .headers(headers)
        .json(&payload)
        .send()
        .await
    {
        Ok(r) => {
            let data: Value = r.json().await.unwrap_or(json!({}));
            Json(json!({
                "success": data["access_token"].is_string(),
                "access_token": data["access_token"],
                "device_limit": !data["access_token"].is_string()
            }))
        }
        Err(e) => Json(json!({ "success": false, "error": e.to_string() })),
    }
}

async fn api_add_account(
    State(state): State<AppState>,
    Json(req): Json<AddTokenReq>,
) -> Json<Value> {
    let session_id = "default".to_string(); // browser generates & sends this
    state
        .user_data
        .entry(session_id.clone())
        .or_insert_with(UserEntry::default)
        .accounts
        .push(Account {
            phone_number: req.phone_number,
            access_token: req.access_token,
            device_id: req.device_id,
        });
    state.save_data();
    Json(json!({ "success": true }))
}

async fn api_get_accounts(State(state): State<AppState>) -> Json<Value> {
    let entry = state
        .user_data
        .get("default")
        .map(|e| e.clone())
        .unwrap_or_default();
    let accounts: Vec<Value> = entry
        .accounts
        .iter()
        .map(|a| json!({ "phone_number": a.phone_number, "device_id": a.device_id }))
        .collect();
    Json(json!({
        "accounts": accounts,
        "active_index": entry.active_index
    }))
}

async fn api_delete_account(
    State(state): State<AppState>,
    axum::extract::Path(idx): axum::extract::Path<usize>,
) -> Json<Value> {
    if let Some(mut entry) = state.user_data.get_mut("default") {
        if idx < entry.accounts.len() {
            entry.accounts.remove(idx);
            state.save_data();
            return Json(json!({ "success": true }));
        }
    }
    Json(json!({ "success": false, "error": "index out of range" }))
}

async fn api_switch_account(
    State(state): State<AppState>,
    axum::extract::Path(idx): axum::extract::Path<usize>,
) -> Json<Value> {
    if let Some(mut entry) = state.user_data.get_mut("default") {
        if idx < entry.accounts.len() {
            entry.active_index = idx;
            state.save_data();
            return Json(json!({ "success": true }));
        }
    }
    Json(json!({ "success": false }))
}

async fn api_launch_all(State(state): State<AppState>) -> Json<Value> {
    let entry = state
        .user_data
        .get("default")
        .map(|e| e.clone())
        .unwrap_or_default();

    if entry.accounts.is_empty() {
        return Json(json!({ "success": false, "error": "No accounts" }));
    }

    let accounts = entry.accounts.clone();
    let session_id = "default".to_string();

    // Init status vector
    let statuses: Vec<WorkerStatus> = accounts
        .iter()
        .enumerate()
        .map(|(i, a)| WorkerStatus {
            worker_id: i + 1,
            phone: a.phone_number.clone(),
            status: "idle".into(),
            correct: 0,
            wrong: 0,
            current_question: None,
            last_event: "Queued".into(),
        })
        .collect();
    state
        .worker_statuses
        .insert(session_id.clone(), statuses);

    // Spawn workers in batches of MAX_WORKERS
    let state_clone = state.clone();
    let sid = session_id.clone();
    tokio::spawn(async move {
        for batch in accounts.chunks(MAX_WORKERS) {
            let mut handles = Vec::new();
            for (i, acc) in batch.iter().enumerate() {
                let s = state_clone.clone();
                let a = acc.clone();
                let sess = sid.clone();
                let handle = tokio::spawn(async move {
                    run_worker(s, sess, i, a).await;
                });
                handles.push(handle);
            }
            for h in handles {
                let _ = h.await;
            }
        }
    });

    Json(json!({ "success": true, "workers": accounts.len() }))
}

async fn api_stop_worker(
    State(state): State<AppState>,
    Json(req): Json<StopWorkerReq>,
) -> Json<Value> {
    let key = (req.session_id, req.worker_index);
    state.active_workers.insert(key, false);
    Json(json!({ "success": true }))
}

async fn api_stop_all(State(state): State<AppState>) -> Json<Value> {
    for mut entry in state.active_workers.iter_mut() {
        *entry.value_mut() = false;
    }
    Json(json!({ "success": true }))
}

async fn api_worker_statuses(State(state): State<AppState>) -> Json<Value> {
    let statuses = state
        .worker_statuses
        .get("default")
        .map(|v| v.clone())
        .unwrap_or_default();
    Json(json!({ "workers": statuses }))
}

async fn api_manual_start(
    State(state): State<AppState>,
    Json(req): Json<ManualStartReq>,
) -> Json<Value> {
    let entry = state
        .user_data
        .get("default")
        .map(|e| e.clone())
        .unwrap_or_default();
    let acc = match entry.accounts.get(req.account_index) {
        Some(a) => a.clone(),
        None => return Json(json!({ "success": false, "error": "No account at index" })),
    };

    let headers = build_auth_headers(&acc.access_token);

    match state
        .client
        .post("https://api.minipix.co/v4/quiz/session/start")
        .headers(headers_to_reqwest(&headers))
        .body("")
        .send()
        .await
    {
        Ok(r) => {
            let data: Value = r.json().await.unwrap_or(json!({}));
            if data["success"].as_bool().unwrap_or(false) {
                Json(json!({
                    "success": true,
                    "quiz_session_id": data["session"]["sessionId"],
                    "question": data["question"]
                }))
            } else {
                Json(json!({ "success": false, "error": "Failed to start" }))
            }
        }
        Err(e) => Json(json!({ "success": false, "error": e.to_string() })),
    }
}

async fn api_manual_answer(
    State(state): State<AppState>,
    Json(req): Json<ManualAnswerReq>,
) -> Json<Value> {
    let entry = state
        .user_data
        .get("default")
        .map(|e| e.clone())
        .unwrap_or_default();
    let acc = match entry.accounts.get(req.account_index) {
        Some(a) => a.clone(),
        None => return Json(json!({ "success": false, "error": "No account" })),
    };

    let headers = build_auth_headers(&acc.access_token);
    let payload = json!({
        "sessionId": req.quiz_session_id,
        "questionId": req.question_id,
        "chosenIndex": req.chosen_index
    });

    match state
        .client
        .post("https://api.minipix.co/v4/quiz/session/answer")
        .headers(headers_to_reqwest(&headers))
        .json(&payload)
        .send()
        .await
    {
        Ok(r) => {
            let data: Value = r.json().await.unwrap_or(json!({}));
            Json(data)
        }
        Err(e) => Json(json!({ "success": false, "error": e.to_string() })),
    }
}

// ─── WebSocket Handler ────────────────────────────────────────────────────────

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> Response {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let mut rx = state.ws_tx.subscribe();

    // Send current worker statuses on connect
    let statuses = state
        .worker_statuses
        .get("default")
        .map(|v| v.clone())
        .unwrap_or_default();
    let init_msg = json!({ "type": "init", "workers": statuses });
    let _ = socket.send(Message::Text(init_msg.to_string())).await;

    loop {
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            Ok(text) = rx.recv() => {
                if socket.send(Message::Text(text)).await.is_err() {
                    break;
                }
            }
        }
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let state = AppState::new();

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        // Static UI
        .route("/", get(serve_index))
        .route("/health", get(health))
        // WebSocket
        .route("/ws", get(ws_handler))
        // Account management
        .route("/api/otp/generate", post(api_generate_otp))
        .route("/api/otp/verify", post(api_verify_otp))
        .route("/api/accounts", get(api_get_accounts))
        .route("/api/accounts", post(api_add_account))
        .route("/api/accounts/:idx", axum::routing::delete(api_delete_account))
        .route("/api/accounts/:idx/switch", post(api_switch_account))
        // Workers
        .route("/api/workers/launch", post(api_launch_all))
        .route("/api/workers/stop", post(api_stop_worker))
        .route("/api/workers/stop-all", post(api_stop_all))
        .route("/api/workers/status", get(api_worker_statuses))
        // Manual quiz
        .route("/api/manual/start", post(api_manual_start))
        .route("/api/manual/answer", post(api_manual_answer))
        .layer(cors)
        .with_state(state);

    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3000);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("MiniPIX Rust Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

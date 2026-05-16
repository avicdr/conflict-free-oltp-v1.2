//! CRDTdb Dashboard Server — multi-peer REST + WebSocket backend.

use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
    extract::ws::{Message, WebSocket},
    http::Method,
    response::Json,
    routing::{get, post},
    Router,
};
use crdtdb::{Engine, FkPolicy};
use futures::{sink::SinkExt, stream::StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;
use tracing_subscriber::fmt::writer::MakeWriterExt;

// ── helpers ──────────────────────────────────────────────────────────────────

macro_rules! lock {
    ($m:expr) => { $m.lock().unwrap() }
}

// ── State ────────────────────────────────────────────────────────────────────

type SharedEngine = Arc<Mutex<Engine>>;

#[derive(Clone)]
struct AppState {
    peers:    Arc<Mutex<HashMap<String, SharedEngine>>>,
    online:   Arc<Mutex<HashMap<String, bool>>>,
    event_tx: broadcast::Sender<Value>,
    chaos:    Arc<Mutex<ChaosConfig>>,
}

#[derive(Clone, Default)]
struct ChaosConfig {
    delay_ms:  u64,
    drop_rate: f64,
}


fn build_state() -> AppState {
    let (event_tx, _) = broadcast::channel(1024);
    let mut peers  = HashMap::new();
    let mut online = HashMap::new();
    let initial_peers = &["P0", "P1", "P2", "P3"];
    for &pid in initial_peers {
        let mut e = Engine::open_with_policy(".", pid, FkPolicy::Tombstone);
        let _ = e.execute("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT)");
        let _ = e.execute("CREATE TABLE orders (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL, total_cents INTEGER NOT NULL)");
        let _ = e.execute("INSERT INTO users VALUES ('u1', 'alice@example.com', 'Alice')");
        let _ = e.execute("INSERT INTO users VALUES ('u2', 'bob@example.com', 'Bob')");
        let _ = e.execute("INSERT INTO orders VALUES ('o1', 'u1', 'PENDING', 2500)");
        let _ = e.execute("INSERT INTO orders VALUES ('o2', 'u2', 'SHIPPED', 8900)");
        peers.insert(pid.to_string(), Arc::new(Mutex::new(e)));
        online.insert(pid.to_string(), true);
    }
    AppState {
        peers:    Arc::new(Mutex::new(peers)),
        online:   Arc::new(Mutex::new(online)),
        event_tx,
        chaos:    Arc::new(Mutex::new(ChaosConfig::default())),
    }
}

fn emit(state: &AppState, event: Value) { let _ = state.event_tx.send(event); }

fn peer_info(state: &AppState, pid: &str) -> Value {
    let p_map = lock!(state.peers);
    if let Some(e_arc) = p_map.get(pid) {
        let e  = lock!(e_arc);
        let on = *lock!(state.online).get(pid).unwrap_or(&true);
        let ops   = e.log.len();
        let hash  = e.snapshot_hash();
        let users  = e.query("SELECT * FROM users").unwrap_or_default().len();
        let orders = e.query("SELECT * FROM orders").unwrap_or_default().len();
        json!({ "id": pid, "online": on, "op_count": ops,
                "snapshot_hash": hash, "user_count": users, "order_count": orders })
    } else {
        json!({ "error": "not found" })
    }
}

fn emit_convergence(state: &AppState) {
    let p_map = lock!(state.peers);
    let mut hashes: HashMap<String, String> = HashMap::new();
    for (p, e_arc) in p_map.iter() {
        hashes.insert(p.clone(), lock!(e_arc).snapshot_hash());
    }
    let vals: Vec<&String> = hashes.values().collect();
    let converged = if vals.is_empty() { true } else { vals.windows(2).all(|w| w[0] == w[1]) };
    emit(state, json!({ "type": "convergence", "hashes": hashes, "converged": converged }));
}

// ── REST handlers ─────────────────────────────────────────────────────────────
#[derive(Deserialize)] struct SyncBody { from: String, to: String }
async fn sync_pair(State(s): State<AppState>, Json(b): Json<SyncBody>) -> Json<Value> {
    let from = b.from; let to = b.to;
    info!("Sync request: {} -> {}", from, to);
    let (fo, to_) = { let on = lock!(s.online);
        (*on.get(&from).unwrap_or(&true), *on.get(&to).unwrap_or(&true)) };
    if !fo || !to_ { 
        info!("Sync failed: One or both peers offline ({}={}, {}={})", from, fo, to, to_);
        return Json(json!({ "ok": false, "error": "peer offline" })); 
    }

    emit(&s, json!({ "type": "sync_start", "from": from, "to": to }));
    let delay = lock!(s.chaos).delay_ms;
    if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }

    {
        let p_map = lock!(s.peers);
        let fe = p_map.get(&from).cloned();
        let te = p_map.get(&to).cloned();
        if let (Some(fe), Some(te)) = (fe, te) {
            let mut fg = lock!(fe); let mut tg = lock!(te);
            fg.sync_with(&mut tg);
        }
    }
    emit(&s, json!({ "type": "sync_done", "from": from, "to": to }));
    emit(&s, json!({ "type": "state_update", "peer": from, "state": peer_info(&s, &from) }));
    emit(&s, json!({ "type": "state_update", "peer": to,   "state": peer_info(&s, &to) }));
    emit_convergence(&s);
    Json(json!({ "ok": true }))
}

async fn get_peers(State(s): State<AppState>) -> Json<Value> {
    let p_map = lock!(s.peers);
    let mut peers: Vec<Value> = Vec::new();
    let mut keys: Vec<String> = p_map.keys().cloned().collect();
    keys.sort();
    for p in keys { peers.push(peer_info(&s, &p)); }
    Json(json!({ "peers": peers }))
}

async fn get_peer_state(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    if !lock!(s.peers).contains_key(&id) { return Json(json!({ "error": "not found" })); }
    Json(peer_info(&s, &id))
}

#[derive(Deserialize)] struct SqlQuery { sql: String }

async fn query_peer(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<SqlQuery>,
) -> Json<Value> {
    info!("Querying peer {}: {}", id, q.sql);
    let eng_arc = { lock!(s.peers).get(&id).cloned() };
    let Some(eng) = eng_arc else { 
        info!("Peer {} not found for query", id);
        return Json(json!({ "error": "not found" })); 
    };
    let result = lock!(eng).query(&q.sql);
    match result {
        Ok(rows) => {
            let json_rows: Vec<Value> = rows.iter().map(|row| {
                let mut obj = serde_json::Map::new();
                for (col, val) in row.columns.iter().zip(row.values.iter()) {
                    obj.insert(col.clone(), val.as_ref()
                        .map(|b| Value::String(String::from_utf8_lossy(b).to_string()))
                        .unwrap_or(Value::Null));
                }
                Value::Object(obj)
            }).collect();
            Json(json!({ "ok": true, "rows": json_rows }))
        }
        Err(e) => Json(json!({ "ok": false, "error": e })),
    }
}

#[derive(Deserialize)]
struct ExecBody {
    sql: String,
    #[serde(default)]
    params: Vec<serde_json::Value>,
}

async fn exec_peer(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<ExecBody>,
) -> Json<Value> {
    info!("Executing on peer {}: {}", id, body.sql);
    let eng_arc = { lock!(s.peers).get(&id).cloned() };
    let Some(eng) = eng_arc else {
        info!("Peer {} not found for exec", id);
        return Json(json!({ "error": "not found" }));
    };

    // Substitute ? placeholders sequentially with quoted SQL literals.
    // This bypasses the broken SqlValue::Placeholder(0) parser bug
    // where all ? markers map to index 0 (empty string parse fails → unwrap_or(0)).
    let sql = if body.params.is_empty() {
        body.sql.clone()
    } else {
        let mut out = String::with_capacity(body.sql.len() + body.params.len() * 8);
        let mut param_iter = body.params.iter();
        for ch in body.sql.chars() {
            if ch == '?' {
                match param_iter.next() {
                    Some(serde_json::Value::String(s)) => {
                        out.push('\'');
                        out.push_str(&s.replace('\'', "''"));
                        out.push('\'');
                    }
                    Some(serde_json::Value::Number(n)) => {
                        out.push_str(&n.to_string());
                    }
                    Some(serde_json::Value::Null) | None => {
                        out.push_str("NULL");
                    }
                    Some(other) => {
                        out.push('\'');
                        out.push_str(&other.to_string().replace('\'', "''"));
                        out.push('\'');
                    }
                }
            } else {
                out.push(ch);
            }
        }
        out
    };

    let result = lock!(eng).execute(&sql);
    let st = peer_info(&s, &id);
    emit(&s, json!({ "type": "op_created", "peer": id, "sql": sql, "state": st }));
    emit_convergence(&s);
    match result {
        Ok(()) => Json(json!({ "ok": true })),
        Err(e) => Json(json!({ "ok": false, "error": e })),
    }
}



async fn sync_all_peers(State(s): State<AppState>) -> Json<Value> {
    info!("Syncing all peers");
    for _ in 0..3 {
        let p_map = lock!(s.peers);
        let keys: Vec<String> = p_map.keys().cloned().collect();
        for i in 0..keys.len() {
            for j in (i+1)..keys.len() {
                let a = &keys[i]; let b = &keys[j];
                let ao = *lock!(s.online).get(a).unwrap_or(&true);
                let bo = *lock!(s.online).get(b).unwrap_or(&true);
                if ao && bo {
                    let ae = p_map[a].clone(); let be = p_map[b].clone();
                    lock!(ae).sync_with(&mut lock!(be));
                }
            }
        }
    }
    let keys: Vec<String> = lock!(s.peers).keys().cloned().collect();
    for p in keys {
        emit(&s, json!({ "type": "state_update", "peer": p.clone(), "state": peer_info(&s, &p) }));
    }
    emit_convergence(&s);
    Json(json!({ "ok": true }))
}

async fn toggle_partition(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    let new_status = { let mut on = lock!(s.online); let cur = *on.get(&id).unwrap_or(&true);
        on.insert(id.clone(), !cur); !cur };
    info!("Toggled partition for peer {}: now {}", id, if new_status { "online" } else { "offline" });
    emit(&s, json!({ "type": "peer_status", "peer": id.clone(), "online": new_status }));

    if new_status {
        info!("Peer {} came back online, automatically syncing with the cluster", id);
        let p_map = lock!(s.peers);
        let on = lock!(s.online);
        let e1 = p_map.get(&id).cloned();
        if let Some(e1) = e1 {
            for (pid, e2) in p_map.iter() {
                if pid != &id && *on.get(pid).unwrap_or(&true) {
                    lock!(e1).sync_with(&mut lock!(e2.clone()));
                }
            }
        }
        drop(on);
        drop(p_map);
        let keys: Vec<String> = lock!(s.peers).keys().cloned().collect();
        for pid in keys {
            emit(&s, json!({ "type": "state_update", "peer": pid.clone(), "state": peer_info(&s, &pid) }));
        }
        emit_convergence(&s);
    }
    
    Json(json!({ "ok": true, "online": new_status }))
}

#[derive(Deserialize)]
struct ChaosBody { delay_ms: Option<u64>, drop_rate: Option<f64> }

async fn set_chaos(State(s): State<AppState>, Json(b): Json<ChaosBody>) -> Json<Value> {
    let mut c = lock!(s.chaos);
    if let Some(v) = b.delay_ms  { c.delay_ms = v; }
    if let Some(v) = b.drop_rate { c.drop_rate = v; }
    Json(json!({ "ok": true }))
}

#[derive(Deserialize)] struct ScenarioBody { name: String }

async fn run_scenario(State(s): State<AppState>, Json(b): Json<ScenarioBody>) -> Json<Value> {
    let name = b.name.clone();
    info!("Running scenario: {}", name);
    emit(&s, json!({ "type": "scenario_start", "name": name }));
    let sc = s.clone(); let n = name.clone();
    tokio::task::spawn_blocking(move || {
        match n.as_str() {
            "concurrent_update" => do_concurrent_update(&sc),
            "delete_vs_update"  => do_delete_vs_update(&sc),
            "uniqueness_storm"  => do_uniqueness_storm(&sc),
            "multi_hop"         => do_multi_hop(&sc),
            "offline_reconnect" => do_offline_reconnect(&sc),
            "chaos_test"        => do_chaos(&sc),
            _                   => {}
        }
        emit_convergence(&sc);
        emit(&sc, json!({ "type": "scenario_done", "name": n }));
    }).await.ok();
    Json(json!({ "ok": true }))
}

async fn reset_cluster(State(s): State<AppState>) -> Json<Value> {
    info!("Resetting entire cluster state — wiping ALL peers");
    // Drop every peer (including benchmark-created scoped peers)
    {
        let mut p_map = lock!(s.peers);
        let mut on_map = lock!(s.online);
        p_map.clear();
        on_map.clear();
        // Re-create only the base dashboard peers with NO pre-seeded data
        for pid in &["P0", "P1", "P2", "P3"] {
            let e = Engine::open_with_policy(".", pid, FkPolicy::Cascade);
            p_map.insert(pid.to_string(), Arc::new(Mutex::new(e)));
            on_map.insert(pid.to_string(), true);
        }
    }
    emit(&s, json!({ "type": "cluster_reset" }));
    emit_convergence(&s);
    Json(json!({ "ok": true }))
}


#[derive(Deserialize)]
struct AddPeerBody {
    peer_id: String,
}
async fn add_peer(
    State(s): State<AppState>,
    Json(body): Json<AddPeerBody>,
) -> Json<Value> {

    let id = body.peer_id;

    let mut p_map = lock!(s.peers);

    if p_map.contains_key(&id) {
        return Json(json!({
            "ok": false,
            "error": "peer already exists"
        }));
    }

    let e = Engine::open_with_policy(".", &id, FkPolicy::Cascade);

    p_map.insert(id.clone(), Arc::new(Mutex::new(e)));

    lock!(s.online).insert(id.clone(), true);

    drop(p_map);

    info!("Added peer {}", id);

    Json(json!({
        "ok": true,
        "id": id
    }))
}

async fn remove_peer(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    info!("Removing peer {}", id);
    let mut p_map = lock!(s.peers);
    if p_map.remove(&id).is_some() {
        lock!(s.online).remove(&id);
        drop(p_map);
        emit(&s, json!({ "type": "peer_removed", "peer": id.clone() }));
        emit_convergence(&s);
        Json(json!({ "ok": true }))
    } else {
        Json(json!({ "error": "not found" }))
    }
}


// ── Scenarios (sync, blocking) ────────────────────────────────────────────────

fn get_active_peers(s: &AppState, count: usize) -> Vec<SharedEngine> {
    let on = lock!(s.online);
    let p_map = lock!(s.peers);
    p_map.iter()
         .filter(|(id, _)| *on.get(*id).unwrap_or(&true))
         .map(|(_, e)| e.clone())
         .take(count)
         .collect()
}

fn do_concurrent_update(s: &AppState) {
    let p = get_active_peers(s, 2);
    if p.len() < 2 { return; }
    let p0 = p[0].clone(); let p1 = p[1].clone();
    let _ = lock!(p0).execute("INSERT INTO users VALUES ('s1','alice@x.com','Alice')");
    lock!(p0).sync_with(&mut lock!(p1));
    let _ = lock!(p0).execute("UPDATE users SET name='Alice Cooper' WHERE id='s1'");
    let _ = lock!(p1).execute("UPDATE users SET name='Alice Prime' WHERE id='s1'");
    lock!(p0).sync_with(&mut lock!(p1));
    // Determine the final value
    let final_val = lock!(p0).query("SELECT name FROM users WHERE id='s1'").ok().and_then(|r| r.get(0).and_then(|row| String::from_utf8(row.values[0].clone().unwrap()).ok())).unwrap_or_default();
    emit(s, json!({ "type": "conflict_detected", "table": "users", "col": "name",
        "peer_a": "Alice Cooper", "peer_b": "Alice Prime",
        "winner": "Determined by HLC timestamp (higher wins)",
        "final_write": final_val,
        "simple": "Two devices edited the same field offline. After reconnect, all agreed on one value." }));
}

fn do_delete_vs_update(s: &AppState) {
    let p = get_active_peers(s, 2);
    if p.len() < 2 { return; }
    let p0 = p[0].clone(); let p1 = p[1].clone();
    let _ = lock!(p0).execute("INSERT INTO users VALUES ('d1','del@x.com','Del')");
    lock!(p0).sync_with(&mut lock!(p1));
    let _ = lock!(p0).execute("DELETE FROM users WHERE id='d1'");
    let _ = lock!(p1).execute("UPDATE users SET email='new@x.com' WHERE id='d1'");
    lock!(p0).sync_with(&mut lock!(p1));
    // Check if d1 is present
    let is_present = lock!(p0).query("SELECT id FROM users WHERE id='d1'").ok().map(|r| !r.is_empty()).unwrap_or(false);
    let final_val = if is_present { "Row Resurrected" } else { "Row Deleted (Tombstone)" };
    emit(s, json!({ "type": "conflict_detected", "table": "users", "row": "d1",
        "type_label": "Delete vs Update",
        "final_write": final_val,
        "policy": "OPTION B: tombstone wins, cell data retained internally" }));
}

fn do_uniqueness_storm(s: &AppState) {
    let on = lock!(s.online);
    let keys: Vec<String> = lock!(s.peers).keys().filter(|k| *on.get(*k).unwrap_or(&true)).cloned().collect();
    drop(on);
    if keys.is_empty() { return; }
    for (i, pid) in keys.iter().enumerate() {
        if let Some(e) = lock!(s.peers).get(pid) {
            let _ = lock!(e).execute(&format!("INSERT INTO users VALUES ('storm{}','storm@x.com','Peer{}')", i, i));
        }
    }
    for _ in 0..5 {
        for i in 0..keys.len() {
            for j in (i+1)..keys.len() {
                let p_map = lock!(s.peers);
                if let (Some(ae), Some(be)) = (p_map.get(&keys[i]), p_map.get(&keys[j])) {
                    let ae = ae.clone(); let be = be.clone();
                    drop(p_map);
                    lock!(ae).sync_with(&mut lock!(be));
                }
            }
        }
    }
    // Final check
    let first_peer = lock!(s.peers).values().next().cloned().unwrap();
    let final_val = lock!(first_peer).query("SELECT name FROM users WHERE email='storm@x.com'").ok().and_then(|r| r.get(0).and_then(|row| String::from_utf8(row.values[0].clone().unwrap()).ok())).unwrap_or_default();
    emit(s, json!({ "type": "conflict_detected", "table": "users", "col": "email",
        "type_label": "Uniqueness Storm", "peers": keys.len(),
        "final_write": final_val,
        "winner": "Deterministic: highest HLC timestamp wins" }));
}

fn do_multi_hop(s: &AppState) {
    let p = get_active_peers(s, 4);
    if p.len() < 4 { return; }
    let _ = lock!(p[0]).execute("INSERT INTO users VALUES ('h1','hop@x.com','Hop')");
    lock!(p[0]).sync_with(&mut lock!(p[1]));
    lock!(p[1]).sync_with(&mut lock!(p[2]));
    lock!(p[2]).sync_with(&mut lock!(p[3]));
    lock!(p[2]).sync_with(&mut lock!(p[1]));
    lock!(p[1]).sync_with(&mut lock!(p[0]));
}

fn do_offline_reconnect(s: &AppState) {
    let keys: Vec<String> = lock!(s.peers).keys().cloned().collect();
    if keys.len() < 2 { return; }
    let p0_id = &keys[0]; let p2_id = &keys[keys.len()-1];
    lock!(s.online).insert(p2_id.clone(), false);
    emit(s, json!({ "type": "peer_status", "peer": p2_id.clone(), "online": false }));
    if let Some(p0) = lock!(s.peers).get(p0_id).cloned() {
        let _ = lock!(p0).execute("INSERT INTO users VALUES ('off1','off@x.com','Offline')");
    }
    std::thread::sleep(Duration::from_millis(800));
    lock!(s.online).insert(p2_id.clone(), true);
    emit(s, json!({ "type": "peer_status", "peer": p2_id.clone(), "online": true }));
    let p_map = lock!(s.peers);
    if let (Some(p0), Some(p2)) = (p_map.get(p0_id).cloned(), p_map.get(p2_id).cloned()) {
        drop(p_map);
        lock!(p0).sync_with(&mut lock!(p2));
    }
}

fn do_chaos(s: &AppState) {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let on = lock!(s.online);
    let keys: Vec<String> = lock!(s.peers).keys().filter(|k| *on.get(*k).unwrap_or(&true)).cloned().collect();
    drop(on);
    if keys.is_empty() { return; }
    for i in 0..12u32 {
        let pid = &keys[rng.gen_range(0..keys.len())];
        if let Some(e) = lock!(s.peers).get(pid).cloned() {
            let _ = lock!(e).execute(&format!("INSERT INTO users VALUES ('c{}','chaos{}@x.com','Chaos{}')", i, i, i));
        }
        if rng.gen_bool(0.4) {
            let a = &keys[rng.gen_range(0..keys.len())];
            let b = &keys[rng.gen_range(0..keys.len())];
            if a != b { 
                let p_map = lock!(s.peers);
                if let (Some(ea), Some(eb)) = (p_map.get(a).cloned(), p_map.get(b).cloned()) {
                    drop(p_map);
                    lock!(ea).sync_with(&mut lock!(eb)); 
                }
            }
        }
        emit(s, json!({ "type": "chaos_op", "step": i, "peer": pid }));
        std::thread::sleep(Duration::from_millis(80));
    }
    for _ in 0..3 {
        for i in 0..keys.len() {
            for j in (i+1)..keys.len() {
                let p_map = lock!(s.peers);
                if let (Some(ea), Some(eb)) = (p_map.get(&keys[i]).cloned(), p_map.get(&keys[j]).cloned()) {
                    drop(p_map);
                    lock!(ea).sync_with(&mut lock!(eb));
                }
            }
        }
    }
}

// ── WebSocket ─────────────────────────────────────────────────────────────────

async fn ws_handler(ws: WebSocketUpgrade, State(s): State<AppState>) -> axum::response::Response {
    ws.on_upgrade(move |socket| handle_ws(socket, s))
}

async fn handle_ws(socket: WebSocket, s: AppState) {
    info!("New WebSocket connection established");
    let (mut sender, mut receiver) = socket.split();
    let mut rx = s.event_tx.subscribe();

    // Send initial state snapshot
    let keys: Vec<String> = lock!(s.peers).keys().cloned().collect();
    let peers: Vec<Value> = keys.iter().map(|p| peer_info(&s, p)).collect();
    let init = json!({ "type": "init", "peers": peers });
    let _ = sender.send(Message::Text(init.to_string())).await;

    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender.send(Message::Text(msg.to_string())).await.is_err() { break; }
        }
    });
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if matches!(msg, Message::Close(_)) { break; }
        }
    });
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
}

// ── Heartbeat ────────────────────────────────────────────────────────────────

async fn heartbeat(s: AppState) {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    loop {
        tick.tick().await;
        emit_convergence(&s);
        let keys: Vec<String> = lock!(s.peers).keys().cloned().collect();
        for p in keys {
            emit(&s, json!({ "type": "heartbeat", "peer": p.clone(), "state": peer_info(&s, &p) }));
        }
    }
}

// ── New Endpoints ─────────────────────────────────────────────────────────────

/// POST /api/peer/:id/sync — sync this peer with all online peers
async fn sync_peer_by_id(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    info!("Syncing peer {} with all online peers", id);
    let p_map = lock!(s.peers);
    let online = lock!(s.online);
    let Some(src) = p_map.get(&id).cloned() else { return Json(json!({ "error": "not found" })); };
    let synced: Vec<String> = p_map.iter()
        .filter(|(pid, _)| *pid != &id && *online.get(*pid).unwrap_or(&true))
        .map(|(pid, e)| { lock!(src).sync_with(&mut lock!(e.clone())); pid.clone() })
        .collect();
    drop(online); drop(p_map);
    emit(&s, json!({ "type": "state_update", "peer": id.clone(), "state": peer_info(&s, &id) }));
    emit_convergence(&s);
    Json(json!({ "ok": true, "synced_with": synced }))
}

/// POST /api/peer/:id/replay — force a full oplog replay (rebuild_store) on this peer
async fn replay_peer(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    info!("Replaying oplog for peer {}", id);
    let eng = { lock!(s.peers).get(&id).cloned() };
    let Some(eng) = eng else { return Json(json!({ "error": "not found" })); };
    let hash_before = lock!(eng).snapshot_hash();
    lock!(eng).rebuild_store();
    let hash_after = lock!(eng).snapshot_hash();
    emit(&s, json!({ "type": "state_update", "peer": id.clone(), "state": peer_info(&s, &id) }));
    Json(json!({ "ok": true, "hash_before": hash_before, "hash_after": hash_after }))
}

/// GET /api/peer/:id/snapshot — return the current BLAKE3 snapshot hash
async fn get_snapshot(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    let eng = { lock!(s.peers).get(&id).cloned() };
    let Some(eng) = eng else { return Json(json!({ "error": "not found" })); };
    let hash = lock!(eng).snapshot_hash();
    Json(json!({ "peer": id, "snapshot_hash": hash }))
}

/// POST /api/peer/:id/connect — explicitly bring a peer online
async fn connect_peer(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    if !lock!(s.peers).contains_key(&id) { return Json(json!({ "error": "not found" })); }
    lock!(s.online).insert(id.clone(), true);
    info!("Peer {} connected (online)", id);
    emit(&s, json!({ "type": "peer_status", "peer": id.clone(), "online": true }));
    let p_map = lock!(s.peers);
    let online = lock!(s.online);
    if let Some(src) = p_map.get(&id).cloned() {
        for (pid, e) in p_map.iter() {
            if pid != &id && *online.get(pid).unwrap_or(&true) {
                lock!(src).sync_with(&mut lock!(e.clone()));
            }
        }
    }
    drop(online); drop(p_map);
    emit(&s, json!({ "type": "state_update", "peer": id.clone(), "state": peer_info(&s, &id) }));
    emit_convergence(&s);
    Json(json!({ "ok": true, "online": true }))
}

/// POST /api/peer/:id/disconnect — explicitly take a peer offline
async fn disconnect_peer(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    if !lock!(s.peers).contains_key(&id) { return Json(json!({ "error": "not found" })); }
    lock!(s.online).insert(id.clone(), false);
    info!("Peer {} disconnected (offline)", id);
    emit(&s, json!({ "type": "peer_status", "peer": id.clone(), "online": false }));
    Json(json!({ "ok": true, "online": false }))
}

/// GET /api/peer/:id/oplog — return the full operation log for a peer
async fn get_oplog(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    let eng = { lock!(s.peers).get(&id).cloned() };
    let Some(eng) = eng else { return Json(json!({ "error": "not found" })); };
    let e = lock!(eng);
    let entries: Vec<Value> = e.log.iter().map(|entry| {
        let hlc = entry.op.hlc();
        json!({
            "seq": entry.seq,
            "op_type": format!("{:?}", entry.op).split(' ').next().unwrap_or("unknown"),
            "table": entry.op.table(),
            "hlc": format!("{}:{}@{}", hlc.wall_ms, hlc.logical, hlc.peer_id),
        })
    }).collect();
    Json(json!({ "peer": id, "op_count": entries.len(), "entries": entries }))
}

#[derive(Deserialize)] struct BootstrapBody { from_peer: String }

/// POST /api/peer/:id/bootstrap — cold-start a peer by pulling all ops from another peer
async fn bootstrap_peer(State(s): State<AppState>, Path(id): Path<String>, Json(b): Json<BootstrapBody>) -> Json<Value> {
    info!("Bootstrapping peer {} from peer {}", id, b.from_peer);
    let (dst, src) = {
        let p = lock!(s.peers);
        (p.get(&id).cloned(), p.get(&b.from_peer).cloned())
    };
    match (dst, src) {
        (Some(dst), Some(src)) => {
            let src_guard = lock!(src);
            lock!(dst).sync_from(&src_guard);
            drop(src_guard);
            let hash = lock!(dst).snapshot_hash();
            emit(&s, json!({ "type": "state_update", "peer": id.clone(), "state": peer_info(&s, &id) }));
            emit_convergence(&s);
            Json(json!({ "ok": true, "bootstrapped": id, "from": b.from_peer, "snapshot_hash": hash }))
        }
        (None, _) => Json(json!({ "error": format!("peer {} not found", id) })),
        (_, None) => Json(json!({ "error": format!("source peer {} not found", b.from_peer) })),
    }
}

#[derive(Deserialize)] struct SchemaBody { sql: String }

/// POST /api/peer/:id/schema — execute DDL (CREATE TABLE) on a peer
async fn create_schema(State(s): State<AppState>, Path(id): Path<String>, Json(b): Json<SchemaBody>) -> Json<Value> {
    info!("Applying schema to peer {}: {}", id, b.sql);
    let eng = { lock!(s.peers).get(&id).cloned() };
    let Some(eng) = eng else { return Json(json!({ "error": "not found" })); };
    let result = lock!(eng).execute(&b.sql).map_err(|e| e.to_string());
    match result {
        Ok(_) => {
            emit(&s, json!({ "type": "state_update", "peer": id.clone(), "state": peer_info(&s, &id) }));
            Json(json!({ "ok": true, "peer": id }))
        }
        Err(e) => Json(json!({ "ok": false, "error": e }))
    }
}

/// GET /api/peer/:id/hash — return the BLAKE3 snapshot hash (simple alias)
async fn get_hash(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    let eng = { lock!(s.peers).get(&id).cloned() };
    let Some(eng) = eng else { return Json(json!({ "error": "not found" })); };
    let hash = lock!(eng).snapshot_hash();
    Json(json!({ "peer": id, "blake3_hash": hash }))
}

/// GET /api/peer/:id/snapshot_state — return full deterministic structured snapshot.
///
/// Returns every table with every visible row, sorted deterministically by table
/// name (lexicographic) then row PK. Also validates:
/// - email uniqueness
/// - FK validity (orders → users)
/// - orphan detection (orders whose user_id has no row at all)
async fn snapshot_state(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    let eng = { lock!(s.peers).get(&id).cloned() };
    let Some(eng) = eng else { return Json(json!({ "error": "not found" })); };
    let e = lock!(eng);

    // 1. Collect all tables in deterministic (sorted) order
    let mut tables_json: serde_json::Map<String, Value> = serde_json::Map::new();

    // Track uniqueness and FK state across tables
    let mut all_emails: Vec<String> = Vec::new();
    let mut user_ids: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut orphaned_orders: Vec<String> = Vec::new();

    // Iterate tables in alphabetical order (BTreeMap guarantees this)
    for (table_name, table_state) in &e.store.tables {
        let col_names: Vec<&str> = table_state.schema.columns.iter()
            .map(|c| c.name.as_str())
            .collect();

        // Get visible rows sorted by PK — ONLY live, non-tombstoned rows
        let mut visible: Vec<&str> = table_state.membership.visible_rows();
        visible.sort_unstable();

        let mut rows_json: Vec<Value> = Vec::new();
        for row_id in visible {
            if let Some(row) = table_state.rows.get(row_id) {
                let mut obj = serde_json::Map::new();
                for col in &col_names {
                    let val = row.read_cell(col)
                        .map(|b| Value::String(String::from_utf8_lossy(b).to_string()))
                        .unwrap_or(Value::Null);
                    obj.insert(col.to_string(), val);
                }
                // Track email for uniqueness check
                if let Some(email) = row.read_cell("email") {
                    all_emails.push(String::from_utf8_lossy(email).to_string());
                }
                // Track user IDs for FK check
                if table_name == "users" {
                    user_ids.insert(row_id.to_string());
                }
                rows_json.push(Value::Object(obj));
            }
        }

        // Check FK orphans in orders table
        if table_name == "orders" {
            for row_value in &rows_json {
                if let Some(uid) = row_value.get("user_id").and_then(|v| v.as_str()) {
                    // Check if user exists at all (even tombstoned counts for FK)
                    let exists = e.store.get_table("users")
                        .map(|t| t.exists_for_fk(uid))
                        .unwrap_or(false);
                    if !exists { orphaned_orders.push(uid.to_string()); }
                }
            }
        }

        tables_json.insert(table_name.clone(), Value::Array(rows_json));
    }

    // 2. Uniqueness validation
    let unique_emails: std::collections::BTreeSet<&String> = all_emails.iter().collect();
    let emails_unique = all_emails.len() == unique_emails.len();

    // 3. FK validity — every visible order's user_id must exist (even as tombstone)
    let fk_valid = orphaned_orders.is_empty();

    // 4. Build integrity report
    let integrity = json!({
        "emails_unique": emails_unique,
        "fk_valid": fk_valid,
        "orphaned_order_user_ids": orphaned_orders,
        "duplicate_emails": if emails_unique { vec![] } else {
            let mut seen = std::collections::BTreeSet::new();
            all_emails.iter().filter(|e| !seen.insert(*e)).cloned().collect::<Vec<_>>()
        },
    });

    Json(Value::Object(tables_json))
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let log_file = std::fs::File::create("dashboard_server.log").unwrap();
    let multi_writer = std::io::stdout.and(log_file);

    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_writer(multi_writer)
        .init();
    let state = build_state();
    tokio::spawn(heartbeat(state.clone()));

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_headers(Any);

    let app = Router::new()
        // ── Existing endpoints ────────────────────────────────────────
        .route("/api/peers",                  get(get_peers))
        .route("/api/peer/:id/state",         get(get_peer_state))
        .route("/api/peer/:id/query",         get(query_peer))
        .route("/api/peer/:id/exec",          post(exec_peer))
        .route("/api/peer/:id/partition",     post(toggle_partition))
        .route("/api/sync",                   post(sync_pair))
        .route("/api/sync/all",               post(sync_all_peers))
        .route("/api/chaos",                  post(set_chaos))
        .route("/api/scenario",               post(run_scenario))
        .route("/api/reset",                  post(reset_cluster))
        .route("/api/peer",                   post(add_peer))
        .route("/api/peer/:id",               axum::routing::delete(remove_peer))
        // ── New endpoints ─────────────────────────────────────────────
        .route("/api/peer/:id/sync",           post(sync_peer_by_id))
        .route("/api/peer/:id/replay",         post(replay_peer))
        .route("/api/peer/:id/snapshot",       get(get_snapshot))
        .route("/api/peer/:id/connect",        post(connect_peer))
        .route("/api/peer/:id/disconnect",     post(disconnect_peer))
        .route("/api/peer/:id/get_state",      get(get_peer_state))
        .route("/api/peer/:id/oplog",          get(get_oplog))
        .route("/api/peer/:id/bootstrap",      post(bootstrap_peer))
        .route("/api/peer/:id/schema",         post(create_schema))
        .route("/api/peer/:id/hash",           get(get_hash))
        .route("/api/peer/:id/snapshot_state", get(snapshot_state))
        .route("/ws",                          get(ws_handler))
        .layer(cors)
        .with_state(state);

    let addr = "0.0.0.0:8888";
    info!("CRDTdb Dashboard Server → http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

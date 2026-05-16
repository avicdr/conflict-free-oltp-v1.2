import sys, re

with open("src/bin/dashboard_server.rs", "r") as f:
    content = f.read()

# 1. State
content = content.replace(
    "peers:    Arc<HashMap<String, SharedEngine>>,",
    "peers:    Arc<Mutex<HashMap<String, SharedEngine>>>,"
)

# 2. Remove PEER_IDS
content = re.sub(r"const PEER_IDS: &\[&str\] = &\[\"P0\", \"P1\", \"P2\", \"P3\"\];\n", "", content)

# 3. build_state
build_state_orig = """    let mut peers  = HashMap::new();
    let mut online = HashMap::new();
    for &pid in PEER_IDS {"""
build_state_new = """    let mut peers  = HashMap::new();
    let mut online = HashMap::new();
    let initial_peers = &["P0", "P1", "P2", "P3"];
    for &pid in initial_peers {"""
content = content.replace(build_state_orig, build_state_new)

content = content.replace(
    "peers:    Arc::new(peers),",
    "peers:    Arc::new(Mutex::new(peers)),"
)

# 4. peer_info
peer_info_orig = """fn peer_info(state: &AppState, pid: &str) -> Value {
    let e  = lock!(state.peers[pid]);
    let on = *lock!(state.online).get(pid).unwrap_or(&true);
    let ops   = e.log.len();
    let hash  = e.snapshot_hash();
    let users  = e.query("SELECT * FROM users").unwrap_or_default().len();
    let orders = e.query("SELECT * FROM orders").unwrap_or_default().len();
    json!({ "id": pid, "online": on, "op_count": ops,
            "snapshot_hash": hash, "user_count": users, "order_count": orders })
}"""
peer_info_new = """fn peer_info(state: &AppState, pid: &str) -> Value {
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
}"""
content = content.replace(peer_info_orig, peer_info_new)

# 5. emit_convergence
conv_orig = """fn emit_convergence(state: &AppState) {
    let hashes: HashMap<String, String> = PEER_IDS.iter()
        .map(|&p| (p.to_string(), lock!(state.peers[p]).snapshot_hash()))
        .collect();
    let vals: Vec<&String> = hashes.values().collect();
    let converged = vals.windows(2).all(|w| w[0] == w[1]);
    emit(state, json!({ "type": "convergence", "hashes": hashes, "converged": converged }));
}"""
conv_new = """fn emit_convergence(state: &AppState) {
    let p_map = lock!(state.peers);
    let mut hashes: HashMap<String, String> = HashMap::new();
    for (p, e_arc) in p_map.iter() {
        hashes.insert(p.clone(), lock!(e_arc).snapshot_hash());
    }
    let vals: Vec<&String> = hashes.values().collect();
    let converged = if vals.is_empty() { true } else { vals.windows(2).all(|w| w[0] == w[1]) };
    emit(state, json!({ "type": "convergence", "hashes": hashes, "converged": converged }));
}"""
content = content.replace(conv_orig, conv_new)

# 6. get_peers
get_peers_orig = """async fn get_peers(State(s): State<AppState>) -> Json<Value> {
    let peers: Vec<Value> = PEER_IDS.iter().map(|&p| peer_info(&s, p)).collect();
    Json(json!({ "peers": peers }))
}"""
get_peers_new = """async fn get_peers(State(s): State<AppState>) -> Json<Value> {
    let p_map = lock!(s.peers);
    let mut peers: Vec<Value> = Vec::new();
    let mut keys: Vec<String> = p_map.keys().cloned().collect();
    keys.sort();
    for p in keys { peers.push(peer_info(&s, &p)); }
    Json(json!({ "peers": peers }))
}"""
content = content.replace(get_peers_orig, get_peers_new)

# 7. get_peer_state
get_peer_state_orig = """async fn get_peer_state(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    if !s.peers.contains_key(&id) { return Json(json!({ "error": "not found" })); }
    Json(peer_info(&s, &id))
}"""
get_peer_state_new = """async fn get_peer_state(State(s): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    if !lock!(s.peers).contains_key(&id) { return Json(json!({ "error": "not found" })); }
    Json(peer_info(&s, &id))
}"""
content = content.replace(get_peer_state_orig, get_peer_state_new)

# 8. query_peer & exec_peer
content = content.replace(
    "let Some(eng) = s.peers.get(&id) else { ",
    "let eng_arc = { lock!(s.peers).get(&id).cloned() };\n    let Some(eng) = eng_arc else { "
)

# 9. sync_pair
sync_pair_orig = """    {
        let fe = s.peers[&from].clone();
        let te = s.peers[&to].clone();
        let mut fg = lock!(fe); let mut tg = lock!(te);
        fg.sync_with(&mut tg);
    }"""
sync_pair_new = """    {
        let p_map = lock!(s.peers);
        let fe = p_map.get(&from).cloned();
        let te = p_map.get(&to).cloned();
        if let (Some(fe), Some(te)) = (fe, te) {
            let mut fg = lock!(fe); let mut tg = lock!(te);
            fg.sync_with(&mut tg);
        }
    }"""
content = content.replace(sync_pair_orig, sync_pair_new)

# 10. sync_all_peers
sync_all_orig = """async fn sync_all_peers(State(s): State<AppState>) -> Json<Value> {
    info!("Syncing all peers");
    for _ in 0..3 {
        for i in 0..PEER_IDS.len() {
            for j in (i+1)..PEER_IDS.len() {
                let a = PEER_IDS[i]; let b = PEER_IDS[j];
                let ao = *lock!(s.online).get(a).unwrap_or(&true);
                let bo = *lock!(s.online).get(b).unwrap_or(&true);
                if ao && bo {
                    let ae = s.peers[a].clone(); let be = s.peers[b].clone();
                    lock!(ae).sync_with(&mut lock!(be));
                }
            }
        }
    }
    for &p in PEER_IDS {
        emit(&s, json!({ "type": "state_update", "peer": p, "state": peer_info(&s, p) }));
    }
    emit_convergence(&s);
    Json(json!({ "ok": true }))
}"""
sync_all_new = """async fn sync_all_peers(State(s): State<AppState>) -> Json<Value> {
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
}"""
content = content.replace(sync_all_orig, sync_all_new)

# 11. toggle_partition
toggle_orig = """    if new_status {
        info!("Peer {} came back online, automatically syncing with the cluster", id);
        let on = lock!(s.online);
        for &pid in PEER_IDS {
            if pid != id && *on.get(pid).unwrap_or(&true) {
                let e1 = s.peers[&id].clone();
                let e2 = s.peers[pid].clone();
                lock!(e1).sync_with(&mut lock!(e2));
            }
        }
        drop(on);
        for &pid in PEER_IDS {
            emit(&s, json!({ "type": "state_update", "peer": pid, "state": peer_info(&s, pid) }));
        }
        emit_convergence(&s);
    }"""
toggle_new = """    if new_status {
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
    }"""
content = content.replace(toggle_orig, toggle_new)

# 12. reset_cluster
reset_orig = """async fn reset_cluster(State(s): State<AppState>) -> Json<Value> {
    info!("Resetting entire cluster state");
    for &pid in PEER_IDS {
        let mut e = Engine::open_with_policy(".", pid, FkPolicy::Tombstone);
        let _ = e.execute("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT)");
        let _ = e.execute("CREATE TABLE orders (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL, total_cents INTEGER NOT NULL)");
        let _ = e.execute("INSERT INTO users VALUES ('u1', 'alice@example.com', 'Alice')");
        let _ = e.execute("INSERT INTO users VALUES ('u2', 'bob@example.com', 'Bob')");
        let _ = e.execute("INSERT INTO orders VALUES ('o1', 'u1', 'PENDING', 2500)");
        let _ = e.execute("INSERT INTO orders VALUES ('o2', 'u2', 'SHIPPED', 8900)");
        
        *lock!(s.peers[pid]) = e;
        lock!(s.online).insert(pid.to_string(), true);
    }
    emit(&s, json!({ "type": "cluster_reset" }));
    for &pid in PEER_IDS {
        emit(&s, json!({ "type": "peer_status", "peer": pid, "online": true }));
        emit(&s, json!({ "type": "state_update", "peer": pid, "state": peer_info(&s, pid) }));
    }
    emit_convergence(&s);
    Json(json!({ "ok": true }))
}"""
reset_new = """async fn reset_cluster(State(s): State<AppState>) -> Json<Value> {
    info!("Resetting entire cluster state");
    let keys: Vec<String> = lock!(s.peers).keys().cloned().collect();
    for pid in &keys {
        let mut e = Engine::open_with_policy(".", pid, FkPolicy::Tombstone);
        let _ = e.execute("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT)");
        let _ = e.execute("CREATE TABLE orders (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL, total_cents INTEGER NOT NULL)");
        let _ = e.execute("INSERT INTO users VALUES ('u1', 'alice@example.com', 'Alice')");
        let _ = e.execute("INSERT INTO users VALUES ('u2', 'bob@example.com', 'Bob')");
        let _ = e.execute("INSERT INTO orders VALUES ('o1', 'u1', 'PENDING', 2500)");
        let _ = e.execute("INSERT INTO orders VALUES ('o2', 'u2', 'SHIPPED', 8900)");
        
        if let Some(arc) = lock!(s.peers).get(pid) {
            *lock!(arc) = e;
        }
        lock!(s.online).insert(pid.clone(), true);
    }
    emit(&s, json!({ "type": "cluster_reset" }));
    for pid in keys {
        emit(&s, json!({ "type": "peer_status", "peer": pid.clone(), "online": true }));
        emit(&s, json!({ "type": "state_update", "peer": pid.clone(), "state": peer_info(&s, &pid) }));
    }
    emit_convergence(&s);
    Json(json!({ "ok": true }))
}

async fn add_peer(State(s): State<AppState>) -> Json<Value> {
    let mut p_map = lock!(s.peers);
    let mut i = 0;
    let id = loop {
        let candidate = format!("P{}", i);
        if !p_map.contains_key(&candidate) { break candidate; }
        i += 1;
    };
    let mut e = Engine::open_with_policy(".", &id, FkPolicy::Tombstone);
    let _ = e.execute("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT)");
    let _ = e.execute("CREATE TABLE orders (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL, total_cents INTEGER NOT NULL)");
    p_map.insert(id.clone(), Arc::new(Mutex::new(e)));
    lock!(s.online).insert(id.clone(), true);
    drop(p_map);
    info!("Added new peer {}", id);
    emit(&s, json!({ "type": "state_update", "peer": id.clone(), "state": peer_info(&s, &id) }));
    emit_convergence(&s);
    Json(json!({ "ok": true, "id": id }))
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
"""
content = content.replace(reset_orig, reset_new)

# 13. Heartbeat
heart_orig = """    loop {
        tick.tick().await;
        emit_convergence(&s);
        for &p in PEER_IDS {
            emit(&s, json!({ "type": "heartbeat", "peer": p, "state": peer_info(&s, p) }));
        }
    }"""
heart_new = """    loop {
        tick.tick().await;
        emit_convergence(&s);
        let keys: Vec<String> = lock!(s.peers).keys().cloned().collect();
        for p in keys {
            emit(&s, json!({ "type": "heartbeat", "peer": p.clone(), "state": peer_info(&s, &p) }));
        }
    }"""
content = content.replace(heart_orig, heart_new)

# 14. handle_ws init state
ws_orig = """    // Send initial state snapshot
    let peers: Vec<Value> = PEER_IDS.iter().map(|&p| peer_info(&s, p)).collect();
    let init = json!({ "type": "init", "peers": peers });"""
ws_new = """    // Send initial state snapshot
    let keys: Vec<String> = lock!(s.peers).keys().cloned().collect();
    let peers: Vec<Value> = keys.iter().map(|p| peer_info(&s, p)).collect();
    let init = json!({ "type": "init", "peers": peers });"""
content = content.replace(ws_orig, ws_new)

# 15. Scenarios & final_write
scenarios_orig = """fn do_concurrent_update(s: &AppState) {
    let p0 = s.peers["P0"].clone(); let p1 = s.peers["P1"].clone();
    let _ = lock!(p0).execute("INSERT INTO users VALUES ('s1','alice@x.com','Alice')");
    lock!(p0).sync_with(&mut lock!(p1));
    let _ = lock!(p0).execute("UPDATE users SET name='Alice Cooper' WHERE id='s1'");
    let _ = lock!(p1).execute("UPDATE users SET name='Alice Prime' WHERE id='s1'");
    emit(s, json!({ "type": "conflict_detected", "table": "users", "col": "name",
        "peer_a": "Alice Cooper", "peer_b": "Alice Prime",
        "winner": "Determined by HLC timestamp (higher wins)",
        "simple": "Two devices edited the same field offline. After reconnect, all agreed on one value." }));
    lock!(p0).sync_with(&mut lock!(p1));
}

fn do_delete_vs_update(s: &AppState) {
    let p0 = s.peers["P0"].clone(); let p1 = s.peers["P1"].clone();
    let _ = lock!(p0).execute("INSERT INTO users VALUES ('d1','del@x.com','Del')");
    lock!(p0).sync_with(&mut lock!(p1));
    let _ = lock!(p0).execute("DELETE FROM users WHERE id='d1'");
    let _ = lock!(p1).execute("UPDATE users SET email='new@x.com' WHERE id='d1'");
    emit(s, json!({ "type": "conflict_detected", "table": "users", "row": "d1",
        "type_label": "Delete vs Update",
        "policy": "OPTION B: tombstone wins, cell data retained internally" }));
    lock!(p0).sync_with(&mut lock!(p1));
}

fn do_uniqueness_storm(s: &AppState) {
    for (i, &pid) in PEER_IDS.iter().enumerate() {
        let _ = lock!(s.peers[pid]).execute(
            &format!("INSERT INTO users VALUES ('storm{}','storm@x.com','Peer{}')", i, i));
    }
    emit(s, json!({ "type": "conflict_detected", "table": "users", "col": "email",
        "type_label": "Uniqueness Storm", "peers": 4,
        "winner": "Deterministic: highest HLC timestamp wins" }));
    for _ in 0..5 {
        for i in 0..PEER_IDS.len() {
            for j in (i+1)..PEER_IDS.len() {
                let ae = s.peers[PEER_IDS[i]].clone(); let be = s.peers[PEER_IDS[j]].clone();
                lock!(ae).sync_with(&mut lock!(be));
            }
        }
    }
}

fn do_multi_hop(s: &AppState) {
    let _ = lock!(s.peers["P0"]).execute("INSERT INTO users VALUES ('h1','hop@x.com','Hop')");
    lock!(s.peers["P0"]).sync_with(&mut lock!(s.peers["P1"]));
    lock!(s.peers["P1"]).sync_with(&mut lock!(s.peers["P2"]));
    lock!(s.peers["P2"]).sync_with(&mut lock!(s.peers["P3"]));
    lock!(s.peers["P2"]).sync_with(&mut lock!(s.peers["P1"]));
    lock!(s.peers["P1"]).sync_with(&mut lock!(s.peers["P0"]));
}

fn do_offline_reconnect(s: &AppState) {
    lock!(s.online).insert("P2".into(), false);
    emit(s, json!({ "type": "peer_status", "peer": "P2", "online": false }));
    let _ = lock!(s.peers["P0"]).execute("INSERT INTO users VALUES ('off1','off@x.com','Offline')");
    std::thread::sleep(Duration::from_millis(800));
    lock!(s.online).insert("P2".into(), true);
    emit(s, json!({ "type": "peer_status", "peer": "P2", "online": true }));
    lock!(s.peers["P0"]).sync_with(&mut lock!(s.peers["P2"]));
}

fn do_chaos(s: &AppState) {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    for i in 0..12u32 {
        let pid = PEER_IDS[rng.gen_range(0..PEER_IDS.len())];
        let _ = lock!(s.peers[pid]).execute(
            &format!("INSERT INTO users VALUES ('c{}','chaos{}@x.com','Chaos{}')", i, i, i));
        if rng.gen_bool(0.4) {
            let a = PEER_IDS[rng.gen_range(0..PEER_IDS.len())];
            let b = PEER_IDS[rng.gen_range(0..PEER_IDS.len())];
            if a != b { lock!(s.peers[a]).sync_with(&mut lock!(s.peers[b])); }
        }
        emit(s, json!({ "type": "chaos_op", "step": i, "peer": pid }));
        std::thread::sleep(Duration::from_millis(80));
    }
    for _ in 0..3 {
        for i in 0..PEER_IDS.len() {
            for j in (i+1)..PEER_IDS.len() {
                lock!(s.peers[PEER_IDS[i]]).sync_with(&mut lock!(s.peers[PEER_IDS[j]]));
            }
        }
    }
}"""
scenarios_new = """fn get_active_peers(s: &AppState, count: usize) -> Vec<SharedEngine> {
    lock!(s.peers).values().take(count).cloned().collect()
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
    let keys: Vec<String> = lock!(s.peers).keys().cloned().collect();
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
    let keys: Vec<String> = lock!(s.peers).keys().cloned().collect();
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
}"""
content = content.replace(scenarios_orig, scenarios_new)

# 16. Routes
routes_orig = """        .route("/api/chaos",             post(set_chaos))
        .route("/api/scenario",          post(run_scenario))
        .route("/api/reset",             post(reset_cluster))
        .route("/ws",                    get(ws_handler))"""
routes_new = """        .route("/api/chaos",             post(set_chaos))
        .route("/api/scenario",          post(run_scenario))
        .route("/api/reset",             post(reset_cluster))
        .route("/api/peer",              post(add_peer))
        .route("/api/peer/:id",          axum::routing::delete(remove_peer))
        .route("/ws",                    get(ws_handler))"""
content = content.replace(routes_orig, routes_new)

with open("src/bin/dashboard_server.rs", "w") as f:
    f.write(content)


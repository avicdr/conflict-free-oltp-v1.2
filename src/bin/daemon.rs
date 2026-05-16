//! CRDTdb HTTP daemon using Axum.
//!
//! Endpoints:
//! - POST /sql  — Execute a SQL statement
//! - POST /sync — Sync with another peer via HTTP
//! - GET /snapshot — Get the current snapshot hash

use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use crdtdb::{Engine, FkPolicy};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

type Db = Arc<Mutex<Engine>>;

#[derive(Deserialize)]
struct SqlRequest {
    stmt: String,
    #[serde(default)]
    #[allow(dead_code)]
    params: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct SqlResponse {
    ok: bool,
    rows: Option<Vec<serde_json::Value>>,
    error: Option<String>,
    rows_affected: usize,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct SyncRequest {
    peer_url: String,
    since: Option<u64>,
}

#[derive(Serialize)]
struct SnapshotResponse {
    hash: String,
    peer_id: String,
}

async fn handle_sql(
    State(db): State<Db>,
    Json(req): Json<SqlRequest>,
) -> (StatusCode, Json<SqlResponse>) {
    let mut engine = db.lock();
    let is_select = req.stmt.trim().to_uppercase().starts_with("SELECT");

    if is_select {
        match engine.query(&req.stmt) {
            Ok(rows) => {
                let json_rows: Vec<serde_json::Value> = rows.iter().map(|row| {
                    let obj: serde_json::Map<String, serde_json::Value> = row.columns.iter()
                        .zip(row.values.iter())
                        .map(|(col, val)| {
                            let v = match val {
                                None => serde_json::Value::Null,
                                Some(b) => {
                                    String::from_utf8(b.clone())
                                        .map(serde_json::Value::String)
                                        .unwrap_or_else(|_| serde_json::Value::String(hex::encode(b)))
                                }
                            };
                            (col.clone(), v)
                        })
                        .collect();
                    serde_json::Value::Object(obj)
                }).collect();

                (StatusCode::OK, Json(SqlResponse {
                    ok: true,
                    rows: Some(json_rows),
                    error: None,
                    rows_affected: 0,
                }))
            }
            Err(e) => (StatusCode::BAD_REQUEST, Json(SqlResponse {
                ok: false, rows: None, error: Some(e), rows_affected: 0,
            })),
        }
    } else {
        match engine.execute(&req.stmt) {
            Ok(()) => (StatusCode::OK, Json(SqlResponse {
                ok: true, rows: None, error: None, rows_affected: 1,
            })),
            Err(e) => (StatusCode::BAD_REQUEST, Json(SqlResponse {
                ok: false, rows: None, error: Some(e), rows_affected: 0,
            })),
        }
    }
}

async fn handle_snapshot(State(db): State<Db>) -> Json<SnapshotResponse> {
    let engine = db.lock();
    Json(SnapshotResponse {
        hash: engine.snapshot_hash(),
        peer_id: engine.peer_id.clone(),
    })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("crdtdb=debug,tower_http=debug")
        .init();

    let peer_id = std::env::var("PEER_ID").unwrap_or_else(|_| "peer_a".to_string());
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let db_path = std::env::var("DB_PATH").unwrap_or_else(|_| "./data".to_string());

    info!("Starting CRDTdb daemon: peer={}, addr={}", peer_id, bind_addr);

    let engine = Engine::open_with_policy(&db_path, &peer_id, FkPolicy::Tombstone);
    let db: Db = Arc::new(Mutex::new(engine));

    let app = Router::new()
        .route("/sql", post(handle_sql))
        .route("/snapshot", get(handle_snapshot))
        .with_state(db);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
    info!("Listening on {}", bind_addr);
    axum::serve(listener, app).await.unwrap();
}

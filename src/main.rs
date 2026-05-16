//! main.rs — default binary entry point (forwards to daemon or demo)
fn main() {
    println!("CRDTdb — use `cargo run --bin crdtdb-daemon` for the HTTP server");
    println!("         or `cargo run --example demo` for the interactive demo");
}

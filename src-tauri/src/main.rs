// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod commands;
use commands::streaming::{start_streaming, start_watching};
use streaming_server::video::client::client_fun;

#[tauri::command]
fn client_call() -> String {
    client_fun()
}

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            client_call,
            start_streaming,
            start_watching
        ])
        .run(tauri::generate_context!())
        .expect("error while running Tauri app");
}

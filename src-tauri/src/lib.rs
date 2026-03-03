pub mod commands;
pub mod state;

use commands::streaming::{start_streaming, start_watching, stop_streaming};
use state::app_state::AppState;
use streaming_server::video::client::client_fun;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tauri::command]
fn client_call() -> String {
    client_fun()
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");

    tauri::Builder::default()
        .manage(AppState::default())
        .invoke_handler(tauri::generate_handler![
            client_call,
            start_streaming,
            stop_streaming,
            start_watching
        ])
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

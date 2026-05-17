pub mod commands;
#[cfg(target_os = "linux")]
pub mod linux_impl;
pub mod network;
pub mod state;
#[cfg(target_os = "windows")]
pub mod windows_impl;

pub mod client;

use commands::streaming::{start_streaming, start_watching, stop_streaming};
use state::app_state::AppState;
use streaming_server::gstreamer as gst;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    gst::init().expect("Error on gsteramer init");
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");

    tauri::Builder::default()
        .manage(AppState::default())
        .invoke_handler(tauri::generate_handler![
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

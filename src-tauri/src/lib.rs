pub mod commands;
#[cfg(target_os = "linux")]
pub mod linux_impl;
pub mod network;
pub mod platform;
pub mod screen;
pub mod state;

use crate::commands::streamer_commands::{
    generate_ticket, start_streaming_direct, start_streaming_iroh, stop_streaming,
};
use crate::commands::viewer_commands::{start_watching_direct, start_watching_iroh, stop_watching};
use state::app_state::AppState;
use streaming_server::gstreamer as gst;
use streaming_server::platform::capture_backend::CaptureBackend;
use tracing_subscriber::EnvFilter;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    const DEFAULT_FILTER: &str = concat!(
        "warn,",
        env!("CARGO_CRATE_NAME"),
        "=debug,streaming_server=debug"
    );

    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| DEFAULT_FILTER.to_string());

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_new(&filter).unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER)),
        )
        .init();

    let iroh_logging = tracing::enabled!(target: "iroh", tracing::Level::DEBUG);
    tracing::info!(%filter, iroh_logging, "logging initialized with filter: ");

    #[cfg(target_os = "windows")]
    let app_state = {
        platform::windows::setup_env();
        AppState::new(CaptureBackend::WindowsGraphicsCapture)
    };

    // Initialize GStreamer safely
    gst::init().expect("Error on gstreamer init");

    tauri::Builder::default()
        .plugin(tauri_plugin_clipboard_manager::init())
        .manage::<AppState>(app_state)
        .invoke_handler(tauri::generate_handler![
            start_streaming_direct,
            start_streaming_iroh,
            stop_streaming,
            start_watching_direct,
            start_watching_iroh,
            stop_watching,
            generate_ticket,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

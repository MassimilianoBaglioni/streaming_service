pub mod commands;
pub mod state;

use commands::streaming::{start_streaming, start_watching, stop_streaming};
use raw_window_handle::{HasDisplayHandle, HasWindowHandle, RawDisplayHandle, RawWindowHandle};
use state::app_state::AppState;
use streaming_server::gstreamer as gst;
use streaming_server::wayland::wayland_handles::WaylandHandles;
use tauri::Manager;
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

fn get_wayland_handles(app: &tauri::AppHandle) -> Option<WaylandHandles> {
    let window = app.get_webview_window("main")?;

    let mut surface_ptr = None;
    let mut display_ptr = None;

    if let Ok(handle) = window.window_handle() {
        if let RawWindowHandle::Wayland(wayland_handle) = handle.as_raw() {
            surface_ptr = Some(wayland_handle.surface.as_ptr() as *mut _);
        }
    }

    if let Ok(handle) = window.display_handle() {
        if let RawDisplayHandle::Wayland(wayland_display) = handle.as_raw() {
            display_ptr = Some(wayland_display.display.as_ptr() as *mut _);
        }
    }

    match (surface_ptr, display_ptr) {
        (Some(surface_ptr), Some(display_ptr)) => Some(WaylandHandles {
            surface_ptr,
            display_ptr,
        }),
        _ => None,
    }
}

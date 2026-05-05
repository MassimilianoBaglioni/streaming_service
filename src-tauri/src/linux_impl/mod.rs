use raw_window_handle::{HasDisplayHandle, HasWindowHandle, RawDisplayHandle, RawWindowHandle};
use streaming_server::video::linux_impl::wayland::wayland_handles::WaylandHandles;
use tauri::Manager;

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

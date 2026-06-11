use tauri::Manager;
use tracing::warn;
use windows::{
    core::Interface,
    Graphics::Capture::{GraphicsCaptureItem, GraphicsCapturePicker},
    Win32::UI::Shell::IInitializeWithWindow,
};
use windows_future::AsyncOperationCompletedHandler;

pub async fn show_picker(app: tauri::AppHandle) -> Option<GraphicsCaptureItem> {
    let hwnd_raw = app.get_webview_window("main").unwrap().hwnd().unwrap().0 as isize;
    let (tx, rx) = std::sync::mpsc::channel();

    app.run_on_main_thread(move || {
        use windows::Win32::Foundation::HWND;

        let hwnd = HWND(hwnd_raw as *mut std::ffi::c_void);

        let picker = GraphicsCapturePicker::new().unwrap();
        unsafe {
            picker
                .cast::<IInitializeWithWindow>()
                .unwrap()
                .Initialize(hwnd)
                .unwrap();
        }

        let async_op = picker
            .PickSingleItemAsync()
            .expect("Failing on pick single item async");

        async_op
            .SetCompleted(&AsyncOperationCompletedHandler::new(move |op, _| {
                let item = op.unwrap().GetResults();

                match item {
                    Ok(val) => {
                        tx.send(Some(val)).unwrap();
                    }
                    Err(e) => warn!("Error on get result: {:?}", e),
                };

                Ok(())
            }))
            .expect("Failing on set SetCompleted");
    })
    .unwrap();

    rx.recv().unwrap()
}

// Sets up to allow bundled distribution on Windows. In this way the program can be shipped without requiring
// Gstreamer installed, but by only running the msi that unpacks required DLLs.
pub fn setup_windows_env() {
    let exe_dir = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();

    let scanner_path = exe_dir
        .join("gst-plugin-scanner.exe")
        .to_string_lossy()
        .replace("\\", "/");
    let plugins_path = exe_dir.join("plugins").to_string_lossy().replace("\\", "/");
    let registry_path = exe_dir
        .join("gstreamer-registry.bin")
        .to_string_lossy()
        .replace("\\", "/");

    println!("cargo:rustc-link-arg=/DELAYLOAD:gstreamer-1.0-0.dll");
    println!("cargo:rustc-link-arg=/DELAYLOAD:gobject-2.0-0.dll");
    println!("cargo:rustc-link-arg=/DELAYLOAD:glib-2.0-0.dll");
    println!("cargo:rustc-link-arg=delayimp.lib");

    std::env::set_var("GST_PLUGIN_SCANNER", &scanner_path);
    std::env::set_var("GST_PLUGIN_PATH", &plugins_path);
    std::env::set_var("GST_REGISTRY", &registry_path);
    std::env::set_var("GST_PLUGIN_SYSTEM_PATH", "");
}

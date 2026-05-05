use tauri::Manager;
use tracing::info;
use windows::{
    core::Interface, Graphics::Capture::GraphicsCapturePicker,
    Win32::UI::Shell::IInitializeWithWindow,
};
use windows_future::AsyncOperationCompletedHandler;

pub async fn show_picker(app: tauri::AppHandle) -> Result<(), String> {
    // Extract as isize plain integer, no thread safety issues
    let hwnd_raw = app.get_webview_window("main").unwrap().hwnd().unwrap().0 as isize;

    app.run_on_main_thread(move || {
        use windows::Win32::Foundation::HWND;

        // Reconstruct HWND inside the closure, on the correct thread
        let hwnd = HWND(hwnd_raw as *mut std::ffi::c_void);

        let picker = GraphicsCapturePicker::new().unwrap();
        unsafe {
            picker
                .cast::<IInitializeWithWindow>()
                .unwrap()
                .Initialize(hwnd)
                .unwrap();
        }
        let async_op = picker.PickSingleItemAsync().unwrap();
        async_op
            .SetCompleted(&AsyncOperationCompletedHandler::new(move |op, _| {
                let item = op.unwrap().GetResults().unwrap();
                info!("User selected: {:?}", item);
                Ok(())
            }))
            .unwrap();
    })
    .unwrap();
    Ok(())
}

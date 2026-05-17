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

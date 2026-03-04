use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread::JoinHandle;

use super::video_source::VideoSource;
use ashpd::WindowIdentifier;
use ashpd::desktop::screencast::{Screencast, SourceType};
use pipewire::spa;
use std::sync::atomic::Ordering::Relaxed;
use tracing::info;

use crate::gui::main_window::{WaylandHandles, create_window};
use crate::video::gs;

pub struct UserData {
    pub format: spa::param::video::VideoInfoRaw,
}
pub struct StreamSession {
    pub node_id: u32,
}
pub struct PipewireSource {
    stop_streaming_flag: Arc<AtomicBool>,
}

impl VideoSource for PipewireSource {
    fn start_streaming(&self) {
        info!("Start streaming video source called");
        self.stop_streaming_flag.store(false, Relaxed);
        self.entry_point_gstreamer();
    }
    fn stop_streaming(&self) {
        info!("Stop streaming video source called");
        self.stop_streaming_flag.store(true, Relaxed);
    }
}

impl PipewireSource {
    pub fn new() -> Self {
        PipewireSource {
            stop_streaming_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Allows the user to pick a screen for the streaming. Returns the node of the created streaming node.
    pub async fn identify_windows(
        window_ptr: *mut std::ffi::c_void,
        surface_ptr: *mut std::ffi::c_void,
    ) -> StreamSession {
        let window_identifier =
            unsafe { WindowIdentifier::from_wayland_raw(surface_ptr, window_ptr) }
                .await
                .unwrap();

        // screencast and session are required to be alive, stream is closed when they are dropped
        let screencast = Screencast::new().await.unwrap();
        let session = screencast.create_session().await.unwrap();

        screencast
            .select_sources(
                &session,
                ashpd::desktop::screencast::CursorMode::Hidden,
                SourceType::Monitor | SourceType::Window,
                false,
                None,
                ashpd::desktop::PersistMode::Application,
            )
            .await
            .unwrap();

        let response = screencast
            .start(&session, Some(&window_identifier))
            .await
            .unwrap();

        let node_id = response
            .response()
            .expect("No streams inside response")
            .streams()
            .get(0)
            .unwrap()
            .pipe_wire_node_id();

        info!("Got node_id from portal: {}", node_id);

        return StreamSession { node_id };
    }

    pub fn entry_point_gstreamer(&self) -> JoinHandle<StreamSession> {
        let (pointers_tx, pointers_rx) = std::sync::mpsc::channel();
        let (close_tx, close_rx) = std::sync::mpsc::channel();

        let flag_clone = self.stop_streaming_flag.clone();

        // Create a thread that waits for Wayland pointers and when received starts the streaming thread.
        let init_streaming_thread_handle = std::thread::spawn(move || {
            /*
                Wait to receive the surface pointers from a window in order to render the screen picker for the user.
                The user, by picking a screen, creates a streaming node from where frames are acquired.
                A window is required to pick a screen, wayland does not allow picking screens to headless processes.
            */
            let handles: WaylandHandles =
                pointers_rx.recv().expect("Could not receive the pointer");

            // Send message to close the window, user picked a screen no longer required.
            close_tx
                .send(())
                .expect("Failed to signal windows to close");

            let rt = tokio::runtime::Runtime::new().unwrap();

            rt.block_on(async {
                let stream_session =
                    PipewireSource::identify_windows(handles.display_ptr, handles.surface_ptr)
                        .await;

                gs::start_screen_stream(stream_session.node_id, flag_clone, "127.0.0.1", "5000")
                    .expect("Error on starting gstreamer server");
                stream_session
            })
        });

        create_window(pointers_tx, close_rx);

        init_streaming_thread_handle
    }
}

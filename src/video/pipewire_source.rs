use std::panic;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use super::video_source::VideoSource;
use ashpd::WindowIdentifier;
use ashpd::desktop::PersistMode;
use ashpd::desktop::screencast::{
    CursorMode, Screencast, SelectSourcesOptions, SourceType, StartCastOptions,
};
use gstreamer::glib::source;
use pipewire::spa;
use std::sync::atomic::Ordering::Relaxed;
use tracing::{error, info, warn};

use crate::network::streaming_event::StreamingEvent;
use crate::network::streaming_events_server::StreamingEventSocketServer;
use crate::video::gs;
use crate::wayland::wayland_handles::WaylandHandles;

pub struct UserData {
    pub format: spa::param::video::VideoInfoRaw,
}
pub struct StreamSession {
    pub node_id: u32,
}
pub struct PipewireSource {
    stop_streaming_flag: Arc<AtomicBool>,
    pointers: Arc<Mutex<Option<WaylandHandles>>>,
    tcp_socket: Option<StreamingEventSocketServer>,
    tcp_address: String,
}

impl VideoSource for PipewireSource {
    fn start_streaming(&mut self) {
        info!("Start streaming video source called");
        // Create the server socket if it doesn't exist yet
        if self.tcp_socket.is_none() {
            self.tcp_socket = Some(
                StreamingEventSocketServer::bind(&self.tcp_address)
                    .expect("Failed to bind tcp socket."),
            );
        }

        // Accept a client (closes previous connection if any and waits for a new one)
        self.tcp_socket
            .as_mut()
            .unwrap()
            .accept()
            .expect("Failed to accept client");
        info!("Accepted client");
        self.stop_streaming_flag.store(false, Relaxed);
        self.entry_point_gstreamer();
    }

    fn stop_streaming(&mut self) {
        info!("Stop streaming video source called");

        self.stop_streaming_flag.store(true, Relaxed);

        let socket = self.tcp_socket.as_mut().unwrap();
        match socket.send_event(&StreamingEvent::End) {
            Ok(_) => info!("Sent End event"),
            Err(e) => warn!("Failed to send End event: {:?}", e),
        }
        socket.disconnect();
    }
}

impl PipewireSource {
    pub fn new(handles: WaylandHandles, addr: String) -> Self {
        PipewireSource {
            stop_streaming_flag: Arc::new(AtomicBool::new(false)),
            pointers: Arc::new(Mutex::new(Some(handles))),
            tcp_address: addr.clone(),
            // TODO this is blocking for the UI, should start in a separate thread.
            tcp_socket: None,
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
                .expect("Failed on window identifier");
        info!("Inside identify windows, post window identifier");

        // screencast and session are required to be alive, stream is closed when they are dropped
        let screencast = Screencast::new().await.expect("Failed screencast");
        info!("Post screencast");

        let session = screencast
            .create_session(Default::default())
            .await
            .expect("Failed session");
        info!("Post session");

        let sources_options = SelectSourcesOptions::default()
            .set_cursor_mode(CursorMode::Embedded)
            .set_multiple(false)
            .set_sources(SourceType::Monitor | SourceType::Window)
            .set_persist_mode(PersistMode::Application);

        screencast
            .select_sources(&session, sources_options)
            .await
            .expect("Failed on select screencast select sources");
        info!("Post select sources");

        // TODO an application can only attempt start a session once.
        let response = screencast
            .start(
                &session,
                Some(&window_identifier),
                StartCastOptions::default(),
            )
            .await
            .expect("Failed on screencast start");
        info!("Post response");

        let node_id = response
            .response()
            .expect("No streams inside response")
            .streams()
            .get(0)
            .expect("Failed on node id retreival")
            .pipe_wire_node_id();

        info!("Got node_id from portal: {}", node_id);

        StreamSession { node_id }
    }

    pub fn entry_point_gstreamer(&mut self) -> JoinHandle<StreamSession> {
        let flag_clone = self.stop_streaming_flag.clone();
        let pointers_clone = self.pointers.clone();

        // Create a thread that waits for Wayland pointers and when received starts the streaming thread.
        let init_streaming_thread_handle = std::thread::spawn(move || {
            info!("Streaming thread entrypoint");

            let handles = match pointers_clone.lock() {
                Ok(guard) => match guard.clone() {
                    Some(h) => h,
                    None => {
                        error!("No wayland handles available");
                        panic!("No wayland handles available");
                    }
                },
                Err(e) => {
                    error!("Failed to lock pointers mutex: {}", e);
                    panic!("Failed to lock pointers mutex: {}", e);
                }
            };
            info!("Post handles");

            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    error!("Failed to create tokio runtime: {}", e);
                    panic!("Failed to create tokio runtime: {}", e);
                }
            };

            rt.block_on(async {
                info!("Pre identify windows");
                let stream_session =
                    PipewireSource::identify_windows(handles.display_ptr, handles.surface_ptr)
                        .await;
                info!("Post identify windows");

                if let Err(e) =
                    gs::start_screen_stream(stream_session.node_id, flag_clone, "127.0.0.1", "5000")
                {
                    error!("Error on starting gstreamer server: {}", e);
                    panic!("Error on starting gstreamer server: {}", e);
                }
                info!("Post start screen stream");

                stream_session
            })
        });

        info!("Entry point gstreamer done.");

        init_streaming_thread_handle
    }
}

use std::net::Ipv4Addr;
use std::panic;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use super::video_source::VideoSource;
use ashpd::WindowIdentifier;
use ashpd::desktop::screencast::{
    CursorMode, Screencast, SelectSourcesOptions, SourceType, StartCastOptions,
};
use ashpd::desktop::{PersistMode, Session};
use pipewire::spa;
use std::sync::atomic::Ordering::Relaxed;
use tokio::runtime::Runtime;
use tracing::{error, info, warn};

use crate::network::streaming_event::StreamingEvent;
use crate::network::streaming_events_server::StreamingEventSocketServer;
use crate::video::gs;
use crate::wayland::wayland_handles::WaylandHandles;

pub struct UserData {
    pub format: spa::param::video::VideoInfoRaw,
}
pub struct PipewireSource {
    stop_streaming_flag: Arc<AtomicBool>,
    pointers: Arc<Mutex<Option<WaylandHandles>>>,
    tcp_socket: Option<StreamingEventSocketServer>,
    tcp_port: u16,
    host_ip: Ipv4Addr,
    streaming_port: u16,
    node_id: Option<u32>,
    session: Option<Session<Screencast>>,
    screencast: Option<Screencast>,
    gstreamer_thread_handle: Option<JoinHandle<()>>,
    rt: Arc<Runtime>,
}

impl VideoSource for PipewireSource {
    fn start_streaming(&mut self) {
        info!("Start streaming video source called");
        // Create the server socket if it doesn't exist yet
        if self.tcp_socket.is_none() {
            self.tcp_socket = Some(
                StreamingEventSocketServer::bind(&format!("{}:{}", self.host_ip, self.tcp_port))
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
        self.gstreamer_thread_handle = Some(self.entry_point_gstreamer());
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

        if let Some(handle) = self.gstreamer_thread_handle.take() {
            handle.join().expect("Failed to await the gs thread.");
        }
        let rt = Arc::clone(&self.rt);

        rt.block_on(self.session_cleanup());
    }

    fn update_network_info(&mut self, host_ip: Ipv4Addr, streaming_port: u16, tcp_port: u16) {
        if !self.tcp_socket.is_none() {
            self.tcp_socket = None;
        }
        self.host_ip = host_ip;
        self.streaming_port = streaming_port;
        self.tcp_port = tcp_port;
    }
}

impl PipewireSource {
    pub fn new(
        handles: WaylandHandles,
        tcp_port: u16,
        streaming_port: u16,
        host_ip: Ipv4Addr,
    ) -> Self {
        let rt = match Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                error!("Failed to create tokio runtime: {}", e);
                panic!("Failed to create tokio runtime: {}", e);
            }
        };

        PipewireSource {
            stop_streaming_flag: Arc::new(AtomicBool::new(false)),
            pointers: Arc::new(Mutex::new(Some(handles))),
            tcp_port,
            host_ip,
            streaming_port,
            // TODO this is blocking for the UI, should start in a separate thread.
            tcp_socket: None,
            session: None,
            screencast: None,
            node_id: None,
            gstreamer_thread_handle: None,
            rt: Arc::new(rt),
        }
    }

    /// Allows the user to pick a screen for the streaming. Returns the node of the created streaming node.
    pub async fn identify_windows(
        &mut self,
        window_ptr: *mut std::ffi::c_void,
        surface_ptr: *mut std::ffi::c_void,
    ) {
        let window_identifier =
            unsafe { WindowIdentifier::from_wayland_raw(surface_ptr, window_ptr) }
                .await
                .expect("Failed on window identifier");
        info!("Inside identify windows, post window identifier");

        if self.screencast.is_none() {
            info!("Pre creation of screencast");
            let screencast = Screencast::new().await.expect("Failed screencast");
            info!("Post screencast");
            self.screencast = Some(screencast);
        }

        if self.session.is_none() {
            info!("Pre new session create");
            let session = self
                .screencast
                .as_mut()
                .unwrap()
                .create_session(Default::default())
                .await
                .expect("Failed session");
            info!("Created new session");
            self.session = Some(session);
        }

        let sources_options = SelectSourcesOptions::default()
            .set_cursor_mode(CursorMode::Embedded)
            .set_multiple(false)
            .set_sources(SourceType::Monitor | SourceType::Window)
            .set_persist_mode(PersistMode::Application);

        info!("Pre select sources");
        self.screencast
            .as_mut()
            .unwrap()
            .select_sources(self.session.as_mut().unwrap(), sources_options)
            .await
            .expect("Failed on select screencast select sources");
        info!("Post select sources");

        let response = self
            .screencast
            .as_mut()
            .unwrap()
            .start(
                self.session.as_mut().unwrap(),
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

        self.node_id = Some(node_id);
    }

    pub fn entry_point_gstreamer(&mut self) -> JoinHandle<()> {
        let flag_clone = self.stop_streaming_flag.clone();
        let pointers_clone = self.pointers.clone();

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

        let rt = Arc::clone(&self.rt);

        rt.block_on(async {
            info!("Pre identify windows");
            self.identify_windows(handles.display_ptr, handles.surface_ptr)
                .await;
            info!("Post identify windows");
        });

        let cloned_node_id = self.node_id.unwrap();
        let streaming_port_clone = self.streaming_port.clone();
        let host_ip_clone = self.host_ip.clone();

        // Create a thread that starts the streaming.
        let init_streaming_thread_handle = std::thread::spawn(move || {
            info!("Streaming thread entrypoint");

            if let Err(e) = gs::start_screen_stream(
                cloned_node_id,
                flag_clone,
                host_ip_clone,
                streaming_port_clone,
            ) {
                error!("Error on starting gstreamer server: {}", e);
                panic!("Error on starting gstreamer server: {}", e);
            }
            info!("Post start screen stream");
        });

        info!("Entry point gstreamer done.");

        init_streaming_thread_handle
    }

    pub async fn session_cleanup(&mut self) {
        info!("Session cleanup pre");
        if let Some(session) = self.session.take() {
            match session.close().await {
                Ok(_) => info!("Closed portal session"),
                Err(e) => warn!("Failed to close portal session: {:?}", e),
            }
        }
        info!("Session cleanup post");

        self.session = None;
        self.screencast = None;
        self.node_id = None;
    }
}

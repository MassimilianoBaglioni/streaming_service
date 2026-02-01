use ashpd::WindowIdentifier;
use ashpd::desktop::screencast::{Screencast, SourceType};
use pipewire::properties::properties;
use pipewire::spa::pod::{Object, Pod};
use pipewire::{self as pw, spa};
use tracing::info;

use crate::gui::main_window::{WaylandHandles, create_window};
use crate::video::{gs, utilities};

pub struct UserData {
    pub format: spa::param::video::VideoInfoRaw,
}
pub struct StreamSession {
    pub node_id: u32,
}
pub struct PipewireSource {}

impl PipewireSource {
    pub fn new() -> Self {
        PipewireSource {}
    }

    pub fn start_streaming(node_id: Option<u32>) -> Result<(), Box<dyn std::error::Error>> {
        pw::init();

        let mainloop = pw::main_loop::MainLoopRc::new(None)?;
        let context = pw::context::ContextRc::new(&mainloop, None)?;
        let core = context.connect_rc(None)?;

        let data = UserData {
            format: Default::default(),
        };

        let stream = pw::stream::StreamBox::new(
            &core,
            "video-test",
            properties! {
                *pw::keys::MEDIA_TYPE => "Video",
                *pw::keys::MEDIA_CATEGORY => "Capture",
                *pw::keys::MEDIA_ROLE => "Camera",
            },
        )?;

        /*let stream = pw::stream::Stream::<UserData>::with_user_data(
            &mainloop,
            "video-test",
            ,
            data,
        )*/

        let _listener = stream
            .add_local_listener_with_user_data(data)
            .state_changed(|_, _, old, new| {
                info!("State changed: {:?} -> {:?}", old, new);
            })
            .param_changed(|_, user_data, id, param| {
                let Some(param) = param else {
                    return;
                };
                if id != pw::spa::param::ParamType::Format.as_raw() {
                    return;
                }

                let (media_type, media_subtype) =
                    match pw::spa::param::format_utils::parse_format(param) {
                        Ok(v) => v,
                        Err(_) => return,
                    };

                if media_type != pw::spa::param::format::MediaType::Video
                    || media_subtype != pw::spa::param::format::MediaSubtype::Raw
                {
                    return;
                }

                user_data
                    .format
                    .parse(param)
                    .expect("Failed to parse param changed to VideoInfoRaw");
                utilities::log_format_data(&user_data);

                // prepare to render video of this size
            })
            .process(move |stream, _| match stream.dequeue_buffer() {
                None => info!("out of buffers"),
                Some(mut buffer) => {
                    let datas = buffer.datas_mut();
                    if datas.is_empty() {
                        return;
                    }

                    if let Some(_frames_data) = datas[0].data() {}
                }
            })
            .register()?;

        let streaming_object = PipewireSource::create_streaming_obj();

        let values: Vec<u8> = pw::spa::pod::serialize::PodSerializer::serialize(
            std::io::Cursor::new(Vec::new()),
            &pw::spa::pod::Value::Object(streaming_object),
        )
        .unwrap()
        .0
        .into_inner();

        let mut params = [Pod::from_bytes(&values).unwrap()];

        stream.connect(
            spa::utils::Direction::Input,
            node_id,
            pw::stream::StreamFlags::AUTOCONNECT | pw::stream::StreamFlags::MAP_BUFFERS,
            &mut params,
        )?;

        mainloop.run();

        Ok(())
    }

    fn create_streaming_obj() -> Object {
        pw::spa::pod::object!(
            pw::spa::utils::SpaTypes::ObjectParamFormat,
            pw::spa::param::ParamType::EnumFormat,
            pw::spa::pod::property!(
                pw::spa::param::format::FormatProperties::MediaType,
                Id,
                pw::spa::param::format::MediaType::Video
            ),
            pw::spa::pod::property!(
                pw::spa::param::format::FormatProperties::MediaSubtype,
                Id,
                pw::spa::param::format::MediaSubtype::Raw
            ),
            pw::spa::pod::property!(
                pw::spa::param::format::FormatProperties::VideoFormat,
                Choice,
                Enum,
                Id,
                pw::spa::param::video::VideoFormat::RGB,
                pw::spa::param::video::VideoFormat::RGB,
                pw::spa::param::video::VideoFormat::RGBA,
                pw::spa::param::video::VideoFormat::RGBx,
                pw::spa::param::video::VideoFormat::BGRx,
                pw::spa::param::video::VideoFormat::YUY2,
                pw::spa::param::video::VideoFormat::I420,
            ),
            pw::spa::pod::property!(
                pw::spa::param::format::FormatProperties::VideoSize,
                Choice,
                Range,
                Rectangle,
                pw::spa::utils::Rectangle {
                    width: 320,
                    height: 240
                },
                pw::spa::utils::Rectangle {
                    width: 1,
                    height: 1
                },
                pw::spa::utils::Rectangle {
                    width: 4096,
                    height: 4096
                }
            ),
            pw::spa::pod::property!(
                pw::spa::param::format::FormatProperties::VideoFramerate,
                Choice,
                Range,
                Fraction,
                pw::spa::utils::Fraction { num: 25, denom: 1 },
                pw::spa::utils::Fraction { num: 0, denom: 1 },
                pw::spa::utils::Fraction {
                    num: 1000,
                    denom: 1
                }
            ),
        )
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

    pub fn entry_point(&self) {
        let (pointers_tx, pointers_rx) = std::sync::mpsc::channel();
        let (close_tx, close_rx) = std::sync::mpsc::channel();

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

                gs::start_screen_stream(stream_session.node_id, "127.0.0.1", "5000")
                    .expect("Error on starting gstreamer server");

                // Schedule the streaming thread with received pointers.
                std::thread::spawn(move || {
                    // !! TEMPORARY COMMENT TO TEST GSTREAMER
                    PipewireSource::start_streaming(Some(stream_session.node_id))
                        .expect("Error on start streaming");
                })
            })
        });

        create_window(pointers_tx, close_rx);

        let streaming_thread_handle = init_streaming_thread_handle
            .join()
            .expect("Could not properly start the streaming thread");

        streaming_thread_handle
            .join()
            .expect("Error on streaming thread join");
    }

    pub fn entry_point_gstreamer(&self) {
        let (pointers_tx, pointers_rx) = std::sync::mpsc::channel();
        let (close_tx, close_rx) = std::sync::mpsc::channel();

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

                info!("Before gs");
                gs::start_screen_stream(stream_session.node_id, "127.0.0.1", "5000")
                    .expect("Error on starting gstreamer server");
                stream_session
            })
        });

        create_window(pointers_tx, close_rx);

        let session = init_streaming_thread_handle
            .join()
            .expect("Could not properly start the streaming thread");

        info!("Node id: {:?}", session.node_id);
    }
}

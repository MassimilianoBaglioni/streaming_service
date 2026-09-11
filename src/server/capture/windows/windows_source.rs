use std::net::IpAddr;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{ UNIX_EPOCH};
use tokio::sync::Mutex;

use crate::network::server_connection::{ServerConnection, ServerConnectionMode};
use crate::capture::{gs, windows::windows_streaming_settings::VideoStreamingSettings};
use gstreamer::prelude::ElementExt;
use gstreamer::Sample;
use gstreamer_app::{AppSink, AppSrc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use windows::{
    core::{IInspectable, Interface, Ref},
    Foundation::TypedEventHandler,
    Graphics::{
        Capture::{Direct3D11CaptureFramePool, GraphicsCaptureItem, GraphicsCaptureSession},
        DirectX::Direct3D11::IDirect3DDevice,
        DirectX::DirectXPixelFormat,
    },
    Win32::{
        Foundation::HMODULE,
        Graphics::{
            Direct3D::D3D_DRIVER_TYPE_HARDWARE,
            Direct3D11::{
                D3D11CreateDevice, ID3D11Device, ID3D11DeviceContext,
                ID3D11Texture2D, D3D11_CPU_ACCESS_READ, D3D11_CREATE_DEVICE_BGRA_SUPPORT, D3D11_SDK_VERSION,
                D3D11_TEXTURE2D_DESC, D3D11_USAGE_STAGING,
            },
            Dxgi::IDXGIDevice,
            Dxgi::{IDXGISurface, DXGI_MAPPED_RECT, DXGI_MAP_READ},
        },
        System::WinRT::Direct3D11::{
            CreateDirect3D11DeviceFromDXGIDevice, IDirect3DDxgiInterfaceAccess,
        },
    },
};
use crate::server::video::windows_source::FrameArrivedHandler;

struct SendWrapper<T>(T);
unsafe impl<T> Send for SendWrapper<T> {}

impl<T> SendWrapper<T> {
    fn new(val: T) -> Self {
        Self(val)
    }
    fn get(&self) -> &T {
        &self.0
    }
}

pub struct WindowsSource {
    connection: Option<Arc<Mutex<ServerConnection>>>,
    graphics_capture_item: Option<SendWrapper<GraphicsCaptureItem>>,
    pub windows_settings: VideoStreamingSettings,
    token: Option<i64>,
    frame_pool: Option<SendWrapper<Direct3D11CaptureFramePool>>,
    graphics_capture_session: Option<SendWrapper<GraphicsCaptureSession>>,
    app_src: Option<Arc<AppSrc>>,
    app_sink: Option<Arc<AppSink>>,
    pipeline: Option<gstreamer::Pipeline>,
    cancel_token: CancellationToken,
}

impl WindowsSource {
    pub fn new(
        server_connection: ServerConnection,
        graphics_capture_item: Option<GraphicsCaptureItem>,
        windows_settings: VideoStreamingSettings,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            connection: Some(Arc::new(Mutex::new(server_connection))),
            graphics_capture_item: graphics_capture_item.map(SendWrapper::new),
            windows_settings,
            token: None,
            frame_pool: None,
            graphics_capture_session: None,
            app_src: None,
            app_sink: None,
            pipeline: None,
            cancel_token: cancellation_token,
        }
    }

    pub async fn start_streaming(&mut self) {
        info!("Start streaming capture source called");
        // Create the server socket if it doesn't exist yet
        self.connection
            .as_ref()
            .unwrap()
            .lock()
            .await
            .accept()
            .await;

        let is_direct = matches!(
            self.connection
                .as_ref()
                .unwrap()
                .lock()
                .await
                .connection_mode,
            ServerConnectionMode::Direct { .. }
        );

        let pixel_format = self.windows_settings.pixel_format;
        let buffer_count = self.windows_settings.buffer_count;
        let capture_item = self.graphics_capture_item.as_ref().unwrap().get().clone();

        let size = capture_item.Size().expect("Failed to get capture item");
        let width = size.Width as u32;
        let height = size.Height as u32;

        self.init_pipeline(width, height).await;

        let mut device: Option<ID3D11Device> = None;
        let mut context: Option<ID3D11DeviceContext> = None;
        unsafe {
            D3D11CreateDevice(
                None,
                D3D_DRIVER_TYPE_HARDWARE,
                HMODULE(null_mut()),
                D3D11_CREATE_DEVICE_BGRA_SUPPORT,
                None,
                D3D11_SDK_VERSION,
                Some(&mut device),
                None,
                Some(&mut context),
            )
            .unwrap()
        }

        let device = device.unwrap();
        let dxgi_device: IDXGIDevice = device.cast().unwrap();
        let context = context.unwrap();
        let inspectable = unsafe { CreateDirect3D11DeviceFromDXGIDevice(&dxgi_device).unwrap() };
        let winrt_device: IDirect3DDevice = inspectable.cast().unwrap();

        let frame_pool = Direct3D11CaptureFramePool::CreateFreeThreaded(
            &winrt_device,
            pixel_format,
            buffer_count,
            capture_item.Size().expect("Error getting size"),
        )
        .expect("Error creating frame pool");

        let staging_desc = D3D11_TEXTURE2D_DESC {
            Width: width,
            Height: height,
            MipLevels: 1,
            ArraySize: 1,
            Format: windows::Win32::Graphics::Dxgi::Common::DXGI_FORMAT_B8G8R8A8_UNORM,
            SampleDesc: windows::Win32::Graphics::Dxgi::Common::DXGI_SAMPLE_DESC {
                Count: 1,
                Quality: 0,
            },
            Usage: D3D11_USAGE_STAGING,
            BindFlags: 0,
            CPUAccessFlags: D3D11_CPU_ACCESS_READ.0 as u32,
            MiscFlags: 0,
        };

        let mut staging_texture: Option<ID3D11Texture2D> = None;
        unsafe {
            device
                .CreateTexture2D(&staging_desc, None, Some(&mut staging_texture))
                .unwrap();
        }
        let staging_texture = staging_texture.unwrap();

        let target_frame_interval =
            std::time::Duration::from_secs_f64(1.0 / self.windows_settings.fps as f64);

        let frame_handler = FrameArrivedHandler {
            app_src: self.app_src.as_ref().unwrap().clone(),
            context,
            staging_texture,
            pixel_format,
            target_frame_interval,
            last_frame_nanos: Arc::new(AtomicU64::new(0)),
        };

        let handler = TypedEventHandler::new(
            move |sender: Ref<Direct3D11CaptureFramePool>, args: Ref<IInspectable>| {
                frame_handler.on_frame_arrived(sender, args)
            },
        );

        let session = frame_pool
            .CreateCaptureSession(&capture_item)
            .expect("Capture session creation failed");

        self.pipeline
            .as_mut()
            .unwrap()
            .set_state(gstreamer::State::Playing)
            .expect("Failed to set pipeline to Playing");

        let token: i64 = frame_pool
            .FrameArrived(&handler)
            .expect("Error registering handler");

        if !is_direct {
            // This code must be always after starting the pipeline because we need a playing pipeline to pull, otherwise we get errors
            let app_sink_clone = self.app_sink.clone();

            WindowsSource::start_sending_frames_iroh(
                app_sink_clone.unwrap(),
                self.connection.clone().unwrap(),
                self.cancel_token.clone(),
            );
        }

        session.StartCapture().expect("Error starting capture");
        self.save_to_keep_alive(frame_pool, session, token);
        info!("Capture started");
    }

    fn save_to_keep_alive(
        &mut self,
        frame_pool: Direct3D11CaptureFramePool,
        capture_session: GraphicsCaptureSession,
        token: i64,
    ) {
        self.frame_pool = Some(SendWrapper::new(frame_pool));
        self.graphics_capture_session = Some(SendWrapper::new(capture_session));
        self.token = Some(token);
    }

    fn start_sending_frames_iroh(
        app_sink: Arc<AppSink>,
        connection: Arc<Mutex<ServerConnection>>,
        cancellation_token: CancellationToken,
    ) {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Sample>(32);

        let cancel_for_pull = cancellation_token.clone();
        let _frame_sender_thread_handle = tokio::task::spawn_blocking(move || {
            loop {
                if cancel_for_pull.is_cancelled() {
                    info!("frame pull thread stopping (cancelled)");
                    break;
                }
                match app_sink.pull_sample() {
                    Ok(sample) => {
                        if sender.blocking_send(sample).is_err() {
                            break; // receiver dropped, nothing to do
                        }
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        warn!("Error on receiving the sample, before sending");
                        break; // EOS or sink shut down — no point looping forever
                    }
                }
            }
        });

        let cancel_for_send = cancellation_token.clone();
        let _iroh_sender_task_handle = tokio::task::spawn(async move {
            tokio::select! {
                _ = cancel_for_send.cancelled() => {
                    info!("iroh sender task cancelled");
                }
                _ = async {
                    let mut conn = connection.lock().await;
                    conn.send_frames_iroh(receiver).await;
                } => {}
            }
        });
    }

    async fn init_pipeline(&mut self, width: u32, height: u32) {
        match self
            .connection
            .as_mut()
            .unwrap()
            .lock()
            .await
            .connection_mode
        {
            ServerConnectionMode::Direct {
                client_address: client_ip,
                client_streaming_port,
                ..
            } => {
                info!(
                    "client_ip: {:?}, port: {:?}",
                    client_ip, client_streaming_port
                );

                let client_ip = match client_ip.ip() {
                    IpAddr::V4(ipv4) => ipv4,
                    IpAddr::V6(_) => {
                        panic!("Expected ipv4")
                    }
                };

                self.pipeline = Some(gs::create_direct_windows_pipeline(
                    width,
                    height,
                    client_ip,
                    client_streaming_port,
                    &self.windows_settings,
                ))
            }
            ServerConnectionMode::Iroh { .. } => {
                self.pipeline = Some(gs::create_windows_pipeline_with_app_dest(
                    width,
                    height,
                    &self.windows_settings,
                ));

                self.app_src = Some(Arc::new(gs::get_app_src(self.pipeline.as_ref().unwrap())));

                self.app_sink = Some(Arc::new(gs::get_app_sink(
                    self.pipeline.as_ref().unwrap(),
                    "rtp_sink",
                )));
            }
        }

        self.app_src = Some(Arc::new(gs::get_app_src(self.pipeline.as_ref().unwrap())));
    }

    pub async fn stop_streaming(&mut self) {
        // stop capture first so no more frames arrive
        if let Some(session) = &self.graphics_capture_session {
            session.get().Close().ok();
        }

        // unregister the frame handler
        if let (Some(pool), Some(token)) = (&self.frame_pool, self.token) {
            pool.get().RemoveFrameArrived(token).ok();
            pool.get().Close().ok();
        }

        // stop gstreamer pipeline
        if let Some(pipeline) = &self.pipeline {
            pipeline.set_state(gstreamer::State::Null).ok();
        }

        // drop everything
        self.graphics_capture_session = None;
        self.frame_pool = None;
        self.token = None;
        self.pipeline = None;
        self.app_src = None;

        self.connection
            .as_mut()
            .expect("No connection established cannot close it")
            .lock()
            .await
            .send_end_event_and_close_conn()
            .await;

        self.connection = None;

        info!("Streaming stopped");
    }

    pub async fn update_network_info(&mut self, server_connection: ServerConnection) {
        *self.connection.as_mut().unwrap().lock().await = server_connection;
    }
}


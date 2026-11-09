use crate::media::element_lookup;
use crate::server::capture::windows::send_wrapper::SendWrapper;
use crate::server::capture::windows::windows_streaming_settings::VideoStreamingSettings;
use gstreamer::prelude::ElementExt;
use gstreamer::Pipeline;
use gstreamer_app::AppSrc;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info};
use windows::core::{IInspectable, Interface, Ref};
use windows::Foundation::TypedEventHandler;
use windows::Graphics::Capture::{
    Direct3D11CaptureFramePool, GraphicsCaptureItem, GraphicsCaptureSession,
};
use windows::Graphics::DirectX::Direct3D11::IDirect3DDevice;
use windows::Graphics::DirectX::DirectXPixelFormat;
use windows::Win32::Foundation::HMODULE;
use windows::Win32::Graphics::Direct3D::D3D_DRIVER_TYPE_HARDWARE;
use windows::Win32::Graphics::Direct3D11::{
    D3D11CreateDevice, ID3D11Device, ID3D11DeviceContext,
    ID3D11Texture2D, D3D11_CPU_ACCESS_READ, D3D11_CREATE_DEVICE_BGRA_SUPPORT, D3D11_SDK_VERSION,
    D3D11_TEXTURE2D_DESC, D3D11_USAGE_STAGING,
};
use windows::Win32::Graphics::Dxgi::{IDXGIDevice, IDXGISurface, DXGI_MAPPED_RECT, DXGI_MAP_READ};
use windows::Win32::System::WinRT::Direct3D11::{
    CreateDirect3D11DeviceFromDXGIDevice, IDirect3DDxgiInterfaceAccess,
};

pub struct WindowsCaptureSource {
    graphics_capture_item: GraphicsCaptureItem,
    video_streaming_settings: VideoStreamingSettings,
    pipeline: Pipeline,
    token: Option<i64>,
    frame_pool: Option<SendWrapper<Direct3D11CaptureFramePool>>,
    graphics_capture_session: Option<SendWrapper<GraphicsCaptureSession>>,
}

impl WindowsCaptureSource {
    pub fn new(
        graphics_capture_item: GraphicsCaptureItem,
        video_streaming_settings: VideoStreamingSettings,
        pipeline: Pipeline,
    ) -> Self {
        Self {
            graphics_capture_item,
            video_streaming_settings,
            pipeline,
            token: None,
            frame_pool: None,
            graphics_capture_session: None,
        }
    }

    pub fn get_capture_item_size(&self) -> (u32, u32) {
        let size = self
            .graphics_capture_item
            .Size()
            .expect("Failed to get capture item");
        let width = size.Width as u32;
        let height = size.Height as u32;
        (width, height)
    }

    pub fn start_streaming(&mut self) {
        let pixel_format = self.video_streaming_settings.pixel_format;
        let buffer_count = self.video_streaming_settings.buffer_count;
        let capture_item = &self.graphics_capture_item;

        let size = capture_item.Size().expect("Failed to get capture item");
        let width = size.Width as u32;
        let height = size.Height as u32;

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
            size,
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
            Duration::from_secs_f64(1.0 / self.video_streaming_settings.fps as f64);

        let app_src = Arc::new(element_lookup::get_app_src(&self.pipeline));

        let frame_handler = FrameArrivedHandler {
            app_src: app_src.as_ref().clone(),
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
            .CreateCaptureSession(capture_item)
            .expect("Capture session creation failed");

        self.pipeline
            .set_state(gstreamer::State::Playing)
            .expect("Failed to set pipeline to Playing");

        let token: i64 = frame_pool
            .FrameArrived(&handler)
            .expect("Error registering handler");

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

    pub fn get_video_streaming_settings(&self) -> VideoStreamingSettings {
        self.video_streaming_settings.clone()
    }

    pub fn stop(&mut self) {
        self.graphics_capture_session
            .as_ref()
            .unwrap()
            .get()
            .Close()
            .ok();
        let frame_pool = self.frame_pool.as_ref().unwrap().get();
        frame_pool.RemoveFrameArrived(self.token.unwrap()).ok();
        frame_pool.Close().ok();

        self.pipeline.set_state(gstreamer::State::Null).ok();

        self.graphics_capture_session = None;
        self.frame_pool = None;
        self.token = None;
    }
}

pub struct FrameArrivedHandler {
    app_src: AppSrc,
    context: ID3D11DeviceContext,
    staging_texture: ID3D11Texture2D,
    pixel_format: DirectXPixelFormat,
    target_frame_interval: std::time::Duration,
    last_frame_nanos: Arc<AtomicU64>,
}

impl FrameArrivedHandler {
    fn on_frame_arrived(
        &self,
        sender: Ref<Direct3D11CaptureFramePool>,
        _args: Ref<IInspectable>,
    ) -> windows::core::Result<()> {
        let frame_pool = sender.unwrap();
        let Ok(frame) = frame_pool.TryGetNextFrame() else {
            return Ok(());
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let last = self.last_frame_nanos.load(Ordering::Relaxed);
        let interval_nanos = self.target_frame_interval.as_nanos() as u64;

        if now - last < interval_nanos {
            return Ok(());
        }
        self.last_frame_nanos.store(now, Ordering::Relaxed);

        let size = match frame.ContentSize() {
            Ok(s) => s,
            Err(e) => {
                error!("ContentSize failed: {:?}", e);
                return Ok(());
            }
        };

        // A surface is an interface to the texture stored in VRAM. The lines below copy the data from VRAM to RAM for the CPU.
        let surface = match frame.Surface() {
            Ok(s) => s,
            Err(e) => {
                error!("Surface failed: {:?}", e);
                return Ok(());
            }
        };

        let dxgi_access: IDirect3DDxgiInterfaceAccess = match surface.cast() {
            Ok(a) => a,
            Err(e) => {
                error!("Cast to IDirect3DDxgiInterfaceAccess failed: {:?}", e);
                return Ok(());
            }
        };

        let capture_texture: ID3D11Texture2D = match unsafe { dxgi_access.GetInterface() } {
            Ok(t) => t,
            Err(e) => {
                error!("GetInterface texture failed: {:?}", e);
                return Ok(());
            }
        };

        // copy from GPU-only capture texture → staging texture (CPU accessible)
        unsafe {
            self.context
                .CopyResource(&self.staging_texture, &capture_texture)
        };

        // map the STAGING texture, not the capture texture
        let dxgi_staging: IDXGISurface = match self.staging_texture.cast() {
            Ok(s) => s,
            Err(e) => {
                error!("Staging cast failed: {:?}", e);
                return Ok(());
            }
        };

        let mut mapped_rect = DXGI_MAPPED_RECT::default();
        if let Err(e) = unsafe { dxgi_staging.Map(&mut mapped_rect, DXGI_MAP_READ) } {
            error!("Map failed: {:?}", e);
            return Ok(());
        }

        let pitch = mapped_rect.Pitch as usize;
        let data_ptr = mapped_rect.pBits;

        let width = size.Width as usize;
        let height = size.Height as usize;
        let bytes_per_pixel = match self.pixel_format {
            DirectXPixelFormat::B8G8R8A8UIntNormalized => 4,
            DirectXPixelFormat::R16G16B16A16Float => 8,
            _ => panic!("Unsupported pixel format"),
        };
        let row_size = width * bytes_per_pixel;
        let total_size = row_size * height;

        // Gstreamer has an internal buffers pooling, this is not an allocation per iteration, no optimization needed.
        let mut gst_buffer = gstreamer::Buffer::with_size(total_size)
            .expect("Failed to allocate the gstreamer buffer");

        {
            let buffer_ref = gst_buffer.get_mut().unwrap();

            let mut map = buffer_ref
                .map_writable()
                .expect("Failed to map GStreamer buffer");
            let dst = map.as_mut_slice();

            for row in 0..height {
                let src_offset = row * pitch;
                let dst_offset = row * row_size;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        data_ptr.add(src_offset),
                        dst.as_mut_ptr().add(dst_offset),
                        row_size,
                    );
                }
            }
        }

        unsafe { dxgi_staging.Unmap().expect("Unmap failed") };

        self.app_src
            .push_buffer(gst_buffer)
            .expect("Failed to push buffer");

        Ok(())
    }
}

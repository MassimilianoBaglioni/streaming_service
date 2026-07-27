use crate::video::commons::scaling_method::ScalingMethod;
use windows::Graphics::DirectX::DirectXPixelFormat;

// TODO add to the UI all these settings, some are still missing
#[derive(Clone)]
pub struct WindowsStreamingSettings {
    pub buffer_count: i32, // The GPU can prepare frames while the display is showing the current one, this is the number of prepared frames
    pub pixel_format: DirectXPixelFormat, // Representation of the pixels, for hdr we should use: R16G16B16A16Float
    pub fps: u32,
    pub bitrate: u32,
    pub resolution: u16,
    pub scaling_method: ScalingMethod,
}

impl Default for WindowsStreamingSettings {
    fn default() -> Self {
        Self {
            buffer_count: 2,
            pixel_format: DirectXPixelFormat::B8G8R8A8UIntNormalized,
            fps: 30,
            bitrate: 10000,
            resolution: 1080,
            scaling_method: ScalingMethod::NearestNeighbour,
        }
    }
}

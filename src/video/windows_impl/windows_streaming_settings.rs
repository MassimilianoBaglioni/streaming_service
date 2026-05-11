use windows::Graphics::DirectX::DirectXPixelFormat;

// TODO allow the user to set this up from the fronted, like increase the number of buffers, or change the pixel format for HDR
pub struct WindowsStreamingSettings {
    pub buffer_count: i32, // The GPU can prepare frames while the display is showing the current one, this is the number of prepared frames
    pub pixel_format: DirectXPixelFormat, // Representation of the pixels, for hdr we should use: R16G16B16A16Float
}

impl Default for WindowsStreamingSettings {
    fn default() -> Self {
        Self {
            buffer_count: 2,
            pixel_format: DirectXPixelFormat::B8G8R8A8UIntNormalized,
        }
    }
}

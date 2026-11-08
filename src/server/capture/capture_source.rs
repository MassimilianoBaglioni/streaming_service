use crate::server::capture::windows::windows_source::WindowsCaptureSource;
use crate::server::capture::windows::windows_streaming_settings::VideoStreamingSettings;

pub enum CaptureSource {
    Windows(WindowsCaptureSource),
}

impl CaptureSource {
    pub fn get_capture_item_size(&self) -> (u32, u32) {
        match self {
            CaptureSource::Windows(windows_source) => windows_source.get_capture_item_size(),
        }
    }

    pub fn get_video_streaming_settings(&self) -> VideoStreamingSettings {
        match self {
            CaptureSource::Windows(windows_source) => windows_source.get_video_streaming_settings(),
        }
    }

    pub fn start_streaming(&mut self) {
        match self {
            CaptureSource::Windows(windows_source) => windows_source.start_streaming(),
        }
    }

    pub fn stop(&mut self) {
        match self {
            CaptureSource::Windows(windows_source) => windows_source.stop(),
        }
    }
}

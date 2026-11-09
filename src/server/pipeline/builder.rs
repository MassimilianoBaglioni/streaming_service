use crate::error::PipelineError;
use crate::platform::capture_backend::CaptureBackend;
use crate::server::network::connection_config::ConnectionConfig;
use crate::server::pipeline::windows::{windows_direct, windows_iroh};
use gstreamer::Pipeline;

pub(crate) static APPSRC_NAME: &str = "src";

pub struct PipelineBuilder {
    capture_backend: CaptureBackend,
}

impl PipelineBuilder {
    pub fn new(capture_backend: CaptureBackend) -> Self {
        Self { capture_backend }
    }

    pub fn build_pipeline(
        &self,
        connection_config: ConnectionConfig,
    ) -> Result<Pipeline, PipelineError> {
        match (self.capture_backend, connection_config) {
            (CaptureBackend::WindowsGraphicsCapture, ConnectionConfig::Iroh(cfg)) => {
                windows_iroh(cfg)
            }
            (CaptureBackend::WindowsGraphicsCapture, ConnectionConfig::DirectConnection(cfg)) => {
                windows_direct(cfg)
            }
        }
    }
}

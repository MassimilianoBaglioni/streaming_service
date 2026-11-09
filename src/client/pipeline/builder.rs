use crate::client::network::connection_config::ConnectionConfig;
use crate::client::pipeline::windows::{windows_direct, windows_iroh};
use crate::error::PipelineError;
use crate::platform::decode_backend::DecodeBackend;
use gstreamer::Pipeline;

pub static APPSRC_NAME: &str = "src";

pub struct Builder {
    decoder: DecodeBackend,
}

impl Builder {
    pub fn new(decoder: DecodeBackend) -> Self {
        Self { decoder }
    }

    pub fn build_pipeline(
        &self,
        connection_config: ConnectionConfig,
    ) -> Result<Pipeline, PipelineError> {
        match (&self.decoder, connection_config) {
            (DecodeBackend::WindowsD3D11, ConnectionConfig::Iroh) => windows_iroh(),
            (DecodeBackend::WindowsD3D11, ConnectionConfig::Direct(cfg)) => {
                windows_direct(cfg.address.port())
            }
        }
    }
}

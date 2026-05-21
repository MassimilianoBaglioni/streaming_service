#[cfg(target_os = "linux")]
use crate::video::linux_impl::pipewire_client::PipewireClient;
#[cfg(target_os = "windows")]
use crate::video::windows_impl::client::windows_client::WindowsClient;
pub enum SpectatorClient {
    #[cfg(target_os = "windows")]
    Windows(WindowsClient),
    #[cfg(target_os = "linux")]
    Pipewire(PipewireClient),
}

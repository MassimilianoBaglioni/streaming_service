#[cfg(target_os = "linux")]
use crate::video::linux_impl::pipewire_source::PipewireSource;
#[cfg(target_os = "windows")]
use crate::video::windows_impl::windows_source::WindowsSource;
pub enum VideoSourceKind {
    #[cfg(target_os = "windows")]
    Windows(WindowsSource),
    #[cfg(target_os = "linux")]
    Pipewire(PipewireSource),
}

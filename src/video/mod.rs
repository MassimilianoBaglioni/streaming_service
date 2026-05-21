pub mod gs;

pub mod spectator_client;
pub mod utilities;
pub mod video_source;

#[cfg(target_os = "windows")]
pub mod windows_impl;

#[cfg(target_os = "linux")]
pub mod linux_impl;

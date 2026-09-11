pub mod gs;

pub mod commons;
pub mod spectator_client;
pub mod video_source;

#[cfg(target_os = "windows")]
pub mod windows;

#[cfg(target_os = "linux")]
pub mod linux;
pub mod scaling_method;

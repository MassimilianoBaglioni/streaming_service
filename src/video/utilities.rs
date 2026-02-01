use crate::video::pipewire_source::UserData;
use tracing::info;

pub fn log_format_data(user_data: &UserData) {
    info!("Video format info:");
    info!(
        "Format: {} ({:?})",
        user_data.format.format().as_raw(),
        user_data.format.format()
    );
    info!(
        "Size: {}x{}",
        user_data.format.size().width,
        user_data.format.size().height
    );
    info!(
        "Framerate: {}/{}",
        user_data.format.framerate().num,
        user_data.format.framerate().denom
    );
}

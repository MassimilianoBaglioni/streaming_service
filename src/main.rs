use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use streaming_server::video::pipewire_source::PipewireSource;

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");

    let pipewire = PipewireSource::new();
    pipewire
        .entry_point_gstreamer()
        .join()
        .expect("Error on streaming thread");
}

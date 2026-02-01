use streaming_server::video::client::receive;
use tracing::{Level, error};
use tracing_subscriber::FmtSubscriber;
pub fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");
    match receive() {
        Ok(()) => {}
        Err(e) => error!("Client error: {:?}", e),
    };
}

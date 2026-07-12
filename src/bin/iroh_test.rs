use anyhow::{Result, anyhow};
use std::env;
use streaming_server::network::iroh::{run_receiver, run_sender};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let mut args = env::args().skip(1);
    let role = args
        .next()
        .ok_or_else(|| anyhow!("expected 'receiver' or 'sender' as the first argument"))?;

    match role.as_str() {
        "receiver" => run_receiver().await,
        "sender" => {
            let ticket_str = args
                .next()
                .ok_or_else(|| anyhow!("expected ticket as the second argument"))?;
            run_sender(&ticket_str).await
        }
        _ => Err(anyhow!(
            "unknown role '{}'; use 'receiver' or 'sender'",
            role
        )),
    }
}

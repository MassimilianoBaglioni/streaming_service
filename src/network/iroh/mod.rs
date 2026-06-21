use anyhow::Result;
use iroh::{Endpoint, endpoint::presets};
use iroh_tickets::endpoint::EndpointTicket;
use std::str::FromStr;

const ALPN: &[u8] = b"myapp/test/1";

pub async fn run_receiver() -> Result<()> {
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;
    endpoint.online().await;

    let ticket = EndpointTicket::new(endpoint.addr());
    println!("Invite ticket: {ticket}");
    println!("Waiting for sender...");

    if let Some(incoming) = endpoint.accept().await {
        let conn = incoming.await?;
        println!("Sender connected!");

        let (mut _send, mut recv) = conn.accept_bi().await?;
        let mut buf = vec![0u8; 1024];
        if let Some(n) = recv.read(&mut buf).await? {
            println!("Received: {}", std::str::from_utf8(&buf[..n])?);
        }
    }
    endpoint.close().await;
    Ok(())
}

pub async fn run_sender(ticket_str: &str) -> Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    endpoint.online().await;

    let ticket = EndpointTicket::from_str(ticket_str)?;
    println!("Connecting...");
    let conn = endpoint
        .connect(ticket.endpoint_addr().clone(), ALPN)
        .await?;
    println!("Connected!");

    let (mut send, _recv) = conn.open_bi().await?;
    send.write_all(b"hello from sender!").await?;
    send.finish()?;
    println!("Message sent!");
    endpoint.close().await;
    Ok(())
}

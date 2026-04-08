use std::net::Ipv4Addr;

pub trait VideoSource: Send + Sync {
    fn start_streaming(&mut self);
    fn stop_streaming(&mut self);
    fn update_network_info(
        &mut self,
        watcher_address: Ipv4Addr,
        streaming_port: u16,
        tcp_port: u16,
    );
}

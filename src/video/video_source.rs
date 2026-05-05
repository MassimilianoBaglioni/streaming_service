use crate::network::NetInfo;

pub trait VideoSource: Send + Sync {
    fn start_streaming(&mut self);
    fn stop_streaming(&mut self);
    fn update_network_info(&mut self, net_info: &NetInfo);
}

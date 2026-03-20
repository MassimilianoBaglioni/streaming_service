pub trait VideoSource: Send + Sync {
    fn start_streaming(&mut self);
    fn stop_streaming(&mut self);
}

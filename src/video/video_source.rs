pub trait VideoSource: Send + Sync {
    fn start_streaming(&self);
    fn stop_streaming(&self);
}

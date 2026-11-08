pub struct SendWrapper<T>(T);
unsafe impl<T> Send for SendWrapper<T> {}

impl<T> SendWrapper<T> {
    pub(crate) fn new(val: T) -> Self {
        Self(val)
    }
    pub(crate) fn get(&self) -> &T {
        &self.0
    }
}

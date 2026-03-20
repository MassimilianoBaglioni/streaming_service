#[derive(Clone)]
pub struct WaylandHandles {
    pub surface_ptr: *mut std::ffi::c_void,
    pub display_ptr: *mut std::ffi::c_void,
}
unsafe impl Send for WaylandHandles {}

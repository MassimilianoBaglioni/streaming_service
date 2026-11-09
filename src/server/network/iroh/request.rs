use crate::server::capture::scaling_method::ScalingMethod;

pub struct IrohConnectionRequest {
    pub scaling_method: ScalingMethod,
    pub resolution: u16,
    pub height: u32,
    pub width: u32,
    pub bitrate: u32,
}

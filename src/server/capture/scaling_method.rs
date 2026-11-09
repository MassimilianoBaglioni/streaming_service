use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum ScalingMethod {
    NearestNeighbour,
    Bilinear,
    Lanczos,
    Mitchell,
}

impl ScalingMethod {
    pub fn as_gst_method(&self) -> u8 {
        match self {
            ScalingMethod::NearestNeighbour => 0,
            ScalingMethod::Bilinear => 1,
            ScalingMethod::Lanczos => 3,
            ScalingMethod::Mitchell => 9,
        }
    }
}

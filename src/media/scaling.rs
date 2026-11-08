pub fn scale_dimensions(resolution: f32, height: u32, width: u32) -> (u32, u32) {
    let aspect_ratio = width as f32 / height as f32;

    let max_height = resolution;
    let max_width = aspect_ratio * max_height;

    let mut scaled_height = height;
    let mut scaled_width = width;

    if height as f32 > max_height {
        scaled_height = max_height as u32;
        scaled_width = (height as f32 * aspect_ratio) as u32;
    } else if width as f32 > max_width {
        scaled_width = max_width as u32;
        scaled_height = (width as f32 * aspect_ratio) as u32;
    }

    // Round to even for H.264
    scaled_width &= !1;
    scaled_height &= !1;

    (scaled_width, scaled_height)
}

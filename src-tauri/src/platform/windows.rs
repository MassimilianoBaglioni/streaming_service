use tracing::info;

// Sets up to allow bundled distribution on Windows. In this way the program can be shipped without requiring
// Gstreamer installed, but by only running the msi that unpacks required DLLs.
pub fn setup_env() {
    let exe_dir = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();

    let scanner_path = exe_dir
        .join("gst-plugin-scanner.exe")
        .to_string_lossy()
        .replace("\\", "/");
    let plugins_path = exe_dir.join("plugins").to_string_lossy().replace("\\", "/");
    let registry_path = exe_dir
        .join("gstreamer-registry.bin")
        .to_string_lossy()
        .replace("\\", "/");

    info!("cargo:rustc-link-arg=/DELAYLOAD:gstreamer-1.0-0.dll");
    info!("cargo:rustc-link-arg=/DELAYLOAD:gobject-2.0-0.dll");
    info!("cargo:rustc-link-arg=/DELAYLOAD:glib-2.0-0.dll");
    info!("cargo:rustc-link-arg=delayimp.lib");

    std::env::set_var("GST_PLUGIN_SCANNER", &scanner_path);
    std::env::set_var("GST_PLUGIN_PATH", &plugins_path);
    std::env::set_var("GST_REGISTRY", &registry_path);
    std::env::set_var("GST_PLUGIN_SYSTEM_PATH", "");
}

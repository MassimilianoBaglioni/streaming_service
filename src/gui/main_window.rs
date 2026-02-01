use raw_window_handle::{HasDisplayHandle, HasWindowHandle, RawDisplayHandle, RawWindowHandle};
use softbuffer::{Context, Surface};
use std::num::NonZeroU32;
use std::rc::Rc;
use tracing::info;
use winit::application::ApplicationHandler;
use winit::event::WindowEvent;
use winit::event_loop::{ActiveEventLoop, ControlFlow, EventLoop};
use winit::window::{Window, WindowId};

pub struct WaylandHandles {
    pub surface_ptr: *mut std::ffi::c_void,
    pub display_ptr: *mut std::ffi::c_void,
}
unsafe impl Send for WaylandHandles {}
struct App {
    window: Option<Rc<Window>>,
    surface: Option<Surface<Rc<Window>, Rc<Window>>>,
    first_draw_done: bool,
    //Channel used to communicate between threads
    surface_ready_sender: Option<std::sync::mpsc::Sender<WaylandHandles>>,
    close_window_receiver: Option<std::sync::mpsc::Receiver<()>>,
}

impl Default for App {
    fn default() -> Self {
        Self {
            window: None,
            surface: None,
            first_draw_done: false,
            surface_ready_sender: None,
            close_window_receiver: None,
        }
    }
}

impl ApplicationHandler for App {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        if self.window.is_none() {
            let window = Rc::new(
                event_loop
                    .create_window(
                        Window::default_attributes()
                            .with_title("Test Window")
                            .with_inner_size(winit::dpi::LogicalSize::new(800.0, 600.0))
                            .with_visible(false),
                    )
                    .unwrap(),
            );

            let mut surface_ptr = None;
            let mut display_ptr = None;

            if let Ok(handle) = window.window_handle() {
                if let RawWindowHandle::Wayland(wayland_handle) = handle.as_raw() {
                    surface_ptr = Some(wayland_handle.surface.as_ptr() as *mut _);
                }
            }

            if let Ok(handle) = window.display_handle() {
                if let RawDisplayHandle::Wayland(wayland_display) = handle.as_raw() {
                    display_ptr = Some(wayland_display.display.as_ptr() as *mut _);
                }
            }

            if let (Some(surf), Some(disp), Some(sender)) =
                (surface_ptr, display_ptr, &self.surface_ready_sender)
            {
                sender
                    .send(WaylandHandles {
                        surface_ptr: surf,
                        display_ptr: disp,
                    })
                    .expect("Failed to send te WaylandHandles");
            }

            let context = Context::new(window.clone()).unwrap();
            let surface = Surface::new(&context, window.clone()).unwrap();

            self.window = Some(window);
            self.surface = Some(surface);

            self.window.as_ref().unwrap().request_redraw();
        }
    }

    fn window_event(&mut self, event_loop: &ActiveEventLoop, _id: WindowId, event: WindowEvent) {
        if let Some(close_rec) = &self.close_window_receiver {
            if close_rec.try_recv().is_ok() {
                info!("Received close signal from picker thread");
                event_loop.exit();
                return;
            }
        }

        match event {
            WindowEvent::CloseRequested => {
                println!("The close button was pressed; stopping");
                event_loop.exit();
            }
            WindowEvent::RedrawRequested => {
                self.draw();
                // self.window.as_ref().unwrap().request_redraw();
            }
            _ => (),
        }
    }
}

impl App {
    fn draw(&mut self) {
        if let (Some(window), Some(surface)) = (&self.window, &mut self.surface) {
            let size = window.inner_size();

            if size.width > 0 && size.height > 0 {
                surface
                    .resize(
                        NonZeroU32::new(size.width).unwrap(),
                        NonZeroU32::new(size.height).unwrap(),
                    )
                    .unwrap();

                let mut buffer = surface.buffer_mut().unwrap();

                // Fill with bright magenta so it's very visible
                let magenta = 0xFFFF00FF; // ARGB format
                for pixel in buffer.iter_mut() {
                    *pixel = magenta;
                }

                buffer.present().unwrap();

                // Make window visible after first successful draw
                if !self.first_draw_done {
                    println!("First draw complete, making window visible");
                    window.set_visible(true);
                    self.first_draw_done = true;
                }
            }
        }
    }
}

pub fn create_window(
    tx: std::sync::mpsc::Sender<WaylandHandles>,
    rec_close: std::sync::mpsc::Receiver<()>,
) {
    let event_loop = EventLoop::new().unwrap();
    event_loop.set_control_flow(ControlFlow::Wait);

    let mut app = App {
        surface_ready_sender: Some(tx),
        close_window_receiver: Some(rec_close),
        ..Default::default()
    };

    let _ = event_loop.run_app(&mut app);
}

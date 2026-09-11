use crate::server::direct_transport::DirectProtocol;
use crate::server::iroh_transport::IrohProtocol;

pub enum ServerProtocol {
    DirectProtocol(DirectProtocol),
    IrohProtocol(IrohProtocol),
}

impl ServerProtocol {
    pub fn start_streaming(&self) {
        match self {
            ServerProtocol::DirectProtocol(direct) => {}
            ServerProtocol::IrohProtocol(iroh) => {
                iroh.start_streaming();
            }
        }
    }
}

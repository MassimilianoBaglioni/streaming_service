use streaming_server::client::client_session::ClientSession;

pub enum ClientState {
    Watching(ClientSession),
    Empty,
}

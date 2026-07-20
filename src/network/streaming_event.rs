use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum StreamingEvent {
    ClientQuit,
    ServerEndsStream,
    GenericError,
}

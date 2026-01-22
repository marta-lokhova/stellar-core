//! IPC module for Core ↔ Overlay communication.

mod messages;
mod transport;

pub use messages::{Message, MessageCodec, MessageType, InvalidMessageType};
pub use transport::{CoreIpc, CoreSender, CoreReceiver, IpcError};

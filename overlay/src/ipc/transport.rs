//! IPC transport over Unix domain sockets.
//!
//! Provides async channel abstraction over blocking Unix socket I/O.

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use super::messages::{Message, MessageCodec, MessageType};

/// Error type for IPC operations
#[derive(Debug)]
pub enum IpcError {
    Io(std::io::Error),
    ConnectionClosed,
    ChannelClosed,
}

impl std::fmt::Display for IpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IpcError::Io(e) => write!(f, "IPC I/O error: {}", e),
            IpcError::ConnectionClosed => write!(f, "IPC connection closed"),
            IpcError::ChannelClosed => write!(f, "IPC channel closed"),
        }
    }
}

impl std::error::Error for IpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            IpcError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for IpcError {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            IpcError::ConnectionClosed
        } else {
            IpcError::Io(e)
        }
    }
}

/// Handle for sending messages to Core.
#[derive(Clone)]
pub struct CoreSender {
    tx: mpsc::UnboundedSender<Message>,
}

impl CoreSender {
    /// Create a new CoreSender (for testing)
    #[cfg(test)]
    pub fn new(tx: mpsc::UnboundedSender<Message>) -> Self {
        Self { tx }
    }
    
    /// Send a message to Core. Never blocks.
    pub fn send(&self, msg: Message) -> Result<(), IpcError> {
        self.tx.send(msg).map_err(|_| IpcError::ChannelClosed)
    }
    
    /// Convenience: send SCP received notification
    pub fn send_scp_received(&self, envelope: Vec<u8>, _from_peer: u64) -> Result<(), IpcError> {
        // TODO: encode peer_id in payload format
        self.send(Message::new(MessageType::ScpReceived, envelope))
    }
    
    /// Convenience: send nomination hash response
    pub fn send_nomination_hash(&self, hash: [u8; 32]) -> Result<(), IpcError> {
        self.send(Message::new(MessageType::NominationHash, hash.to_vec()))
    }
    
    /// Convenience: send TX set available notification
    pub fn send_tx_set_available(&self, hash: [u8; 32], xdr: Vec<u8>) -> Result<(), IpcError> {
        // Payload: [hash:32][xdr...]
        let mut payload = Vec::with_capacity(32 + xdr.len());
        payload.extend_from_slice(&hash);
        payload.extend_from_slice(&xdr);
        self.send(Message::new(MessageType::TxSetAvailable, payload))
    }
}

/// Handle for receiving messages from Core.
pub struct CoreReceiver {
    rx: mpsc::UnboundedReceiver<Message>,
}

impl CoreReceiver {
    /// Receive a message from Core. Async.
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}

/// Manages the IPC connection to Core.
///
/// Spawns background tasks for reading/writing to the Unix socket.
/// Provides async channels for the rest of the overlay to use.
pub struct CoreIpc {
    /// Sender for outgoing messages
    pub sender: CoreSender,
    /// Receiver for incoming messages
    pub receiver: CoreReceiver,
    /// Join handle for reader task
    reader_handle: tokio::task::JoinHandle<()>,
    /// Join handle for writer task  
    writer_handle: tokio::task::JoinHandle<()>,
}

impl CoreIpc {
    /// Connect to Core's IPC socket (client mode).
    pub async fn connect<P: AsRef<Path>>(socket_path: P) -> Result<Self, IpcError> {
        let path = socket_path.as_ref();
        info!("Connecting to Core IPC socket: {}", path.display());
        
        // Connect (blocking, but fast for Unix sockets)
        let stream = UnixStream::connect(path)?;
        stream.set_nonblocking(false)?; // We use blocking I/O in spawn_blocking
        
        Self::from_stream(stream)
    }
    
    /// Listen on socket and accept one connection (server mode).
    /// This is used when overlay starts first and Core connects to it.
    pub async fn listen<P: AsRef<Path>>(socket_path: P) -> Result<Self, IpcError> {
        let path = socket_path.as_ref();
        
        // Remove existing socket file if present
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        
        info!("Listening for Core connection on: {}", path.display());
        
        // Create listener (blocking, but we only accept once)
        let listener = UnixListener::bind(path)?;
        
        // Accept one connection (blocking)
        let (stream, _) = tokio::task::spawn_blocking(move || {
            listener.accept()
        }).await.map_err(|e| IpcError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )))??;
        
        info!("Core connected");
        stream.set_nonblocking(false)?;
        
        Self::from_stream(stream)
    }
    
    /// Create from existing Unix stream (for testing).
    pub fn from_stream(stream: UnixStream) -> Result<Self, IpcError> {
        let stream = Arc::new(stream);
        
        // Channels for async communication
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<Message>();
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel::<Message>();
        
        // Spawn reader task
        let reader_stream = Arc::clone(&stream);
        let reader_handle = tokio::spawn(async move {
            Self::reader_loop(reader_stream, inbound_tx).await;
        });
        
        // Spawn writer task
        let writer_stream = Arc::clone(&stream);
        let writer_handle = tokio::spawn(async move {
            Self::writer_loop(writer_stream, outbound_rx).await;
        });
        
        Ok(Self {
            sender: CoreSender { tx: outbound_tx },
            receiver: CoreReceiver { rx: inbound_rx },
            reader_handle,
            writer_handle,
        })
    }
    
    /// Reader loop: blocking read in spawn_blocking, forward to channel.
    async fn reader_loop(
        stream: Arc<UnixStream>,
        tx: mpsc::UnboundedSender<Message>,
    ) {
        loop {
            // Clone for the blocking task
            let stream = Arc::clone(&stream);
            
            // Read one message (blocking)
            let result = tokio::task::spawn_blocking(move || {
                // We need to get a &mut, but we have Arc<UnixStream>
                // UnixStream implements Read for &UnixStream, so this works
                let mut reader = &*stream;
                MessageCodec::read(&mut reader)
            })
            .await;
            
            match result {
                Ok(Ok(msg)) => {
                    debug!("IPC received: {:?} ({} bytes)", msg.msg_type, msg.payload.len());
                    if tx.send(msg).is_err() {
                        debug!("IPC reader: channel closed, stopping");
                        break;
                    }
                }
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    info!("Core IPC connection closed");
                    break;
                }
                Ok(Err(e)) => {
                    error!("IPC read error: {}", e);
                    break;
                }
                Err(e) => {
                    error!("IPC reader task panicked: {}", e);
                    break;
                }
            }
        }
    }
    
    /// Writer loop: receive from channel, blocking write.
    async fn writer_loop(
        stream: Arc<UnixStream>,
        mut rx: mpsc::UnboundedReceiver<Message>,
    ) {
        while let Some(msg) = rx.recv().await {
            let stream = Arc::clone(&stream);
            let msg_type = msg.msg_type;
            let payload_len = msg.payload.len();
            
            // Write one message (blocking)
            let result = tokio::task::spawn_blocking(move || {
                let mut writer = &*stream;
                MessageCodec::write(&mut writer, &msg)
            })
            .await;
            
            match result {
                Ok(Ok(())) => {
                    debug!("IPC sent: {:?} ({} bytes)", msg_type, payload_len);
                }
                Ok(Err(e)) => {
                    error!("IPC write error: {}", e);
                    break;
                }
                Err(e) => {
                    error!("IPC writer task panicked: {}", e);
                    break;
                }
            }
        }
        
        debug!("IPC writer: channel closed, stopping");
    }
    
    /// Gracefully shutdown the IPC connection.
    pub async fn shutdown(self) {
        // Dropping sender will close the writer loop
        drop(self.sender);
        
        // Wait for tasks to finish (with timeout)
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            async {
                let _ = self.writer_handle.await;
                let _ = self.reader_handle.await;
            }
        ).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream as StdUnixStream;
    
    #[tokio::test]
    async fn test_ipc_roundtrip() {
        // Create a socket pair
        let (s1, s2) = StdUnixStream::pair().unwrap();
        
        // Create IPC from one end
        let ipc = CoreIpc::from_stream(s1).unwrap();
        
        // Send from the other end (simulating Core)
        let mut core_side = s2;
        let msg = Message::new(MessageType::BroadcastScp, vec![1, 2, 3]);
        MessageCodec::write(&mut core_side, &msg).unwrap();
        
        // Receive on overlay side
        let mut receiver = ipc.receiver;
        let received = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            receiver.recv()
        ).await.unwrap().unwrap();
        
        assert_eq!(received.msg_type, MessageType::BroadcastScp);
        assert_eq!(received.payload, vec![1, 2, 3]);
        
        // Send from overlay side
        ipc.sender.send(Message::new(MessageType::ScpReceived, vec![4, 5, 6])).unwrap();
        
        // Small delay for write to complete
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        
        // Receive on Core side
        let received = MessageCodec::read(&mut core_side).unwrap();
        assert_eq!(received.msg_type, MessageType::ScpReceived);
        assert_eq!(received.payload, vec![4, 5, 6]);
    }
}

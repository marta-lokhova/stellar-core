//! Noise protocol authentication for peer connections (Phase 4).
//!
//! Implements Noise_XX handshake for mutual authentication between overlay nodes.
//! 
//! Noise_XX pattern:
//!   -> e                     (initiator sends ephemeral)
//!   <- e, ee, s, es          (responder sends ephemeral, static)
//!   -> s, se                 (initiator sends static)
//!
//! After handshake completes, both sides have:
//! - Transport encryption keys
//! - Authenticated remote public key

use std::io;
use snow::{Builder, HandshakeState, TransportState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, trace};

/// Noise protocol pattern we use
const NOISE_PATTERN: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2b";

/// Maximum size of a handshake message
const MAX_HANDSHAKE_MSG_SIZE: usize = 256;

/// Static keypair for this node (generated or loaded from config)
pub struct NoiseKeypair {
    /// Private key (32 bytes)
    pub private: [u8; 32],
    /// Public key (32 bytes)
    pub public: [u8; 32],
}

impl NoiseKeypair {
    /// Generate a new random keypair
    pub fn generate() -> Self {
        let builder = Builder::new(NOISE_PATTERN.parse().unwrap());
        let keypair = builder.generate_keypair().unwrap();
        
        let mut private = [0u8; 32];
        let mut public = [0u8; 32];
        private.copy_from_slice(&keypair.private);
        public.copy_from_slice(&keypair.public);
        
        Self { private, public }
    }
    
    /// Create from existing key bytes
    pub fn from_bytes(private: [u8; 32], public: [u8; 32]) -> Self {
        Self { private, public }
    }
}

/// Result of a completed handshake
pub struct NoiseSession {
    /// Transport state for encrypt/decrypt
    transport: TransportState,
    /// Remote peer's static public key
    remote_public_key: [u8; 32],
}

impl NoiseSession {
    /// Get the remote peer's public key
    pub fn remote_public_key(&self) -> &[u8; 32] {
        &self.remote_public_key
    }
    
    /// Encrypt a message for sending
    pub fn encrypt(&mut self, plaintext: &[u8]) -> io::Result<Vec<u8>> {
        let mut ciphertext = vec![0u8; plaintext.len() + 16]; // 16 byte auth tag
        let len = self.transport
            .write_message(plaintext, &mut ciphertext)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        ciphertext.truncate(len);
        Ok(ciphertext)
    }
    
    /// Decrypt a received message
    pub fn decrypt(&mut self, ciphertext: &[u8]) -> io::Result<Vec<u8>> {
        let mut plaintext = vec![0u8; ciphertext.len()];
        let len = self.transport
            .read_message(ciphertext, &mut plaintext)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        plaintext.truncate(len);
        Ok(plaintext)
    }
}

/// Perform handshake as initiator (outbound connection).
pub async fn handshake_initiator<S>(
    stream: &mut S,
    local_keypair: &NoiseKeypair,
) -> io::Result<NoiseSession>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let builder = Builder::new(NOISE_PATTERN.parse().unwrap())
        .local_private_key(&local_keypair.private);
    
    let mut state = builder
        .build_initiator()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    
    // -> e
    let msg1 = write_handshake_msg(&mut state, &[])?;
    send_handshake_msg(stream, &msg1).await?;
    debug!("Sent handshake msg 1 (-> e)");
    
    // <- e, ee, s, es
    let msg2 = recv_handshake_msg(stream).await?;
    read_handshake_msg(&mut state, &msg2)?;
    debug!("Received handshake msg 2 (<- e, ee, s, es)");
    
    // -> s, se
    let msg3 = write_handshake_msg(&mut state, &[])?;
    send_handshake_msg(stream, &msg3).await?;
    debug!("Sent handshake msg 3 (-> s, se)");
    
    // Extract transport and remote key
    let remote_public_key = extract_remote_public_key(&state)?;
    let transport = state
        .into_transport_mode()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    
    debug!("Handshake complete (initiator)");
    Ok(NoiseSession { transport, remote_public_key })
}

/// Perform handshake as responder (inbound connection).
pub async fn handshake_responder<S>(
    stream: &mut S,
    local_keypair: &NoiseKeypair,
) -> io::Result<NoiseSession>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let builder = Builder::new(NOISE_PATTERN.parse().unwrap())
        .local_private_key(&local_keypair.private);
    
    let mut state = builder
        .build_responder()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    
    // <- e
    let msg1 = recv_handshake_msg(stream).await?;
    read_handshake_msg(&mut state, &msg1)?;
    debug!("Received handshake msg 1 (<- e)");
    
    // -> e, ee, s, es
    let msg2 = write_handshake_msg(&mut state, &[])?;
    send_handshake_msg(stream, &msg2).await?;
    debug!("Sent handshake msg 2 (-> e, ee, s, es)");
    
    // <- s, se
    let msg3 = recv_handshake_msg(stream).await?;
    read_handshake_msg(&mut state, &msg3)?;
    debug!("Received handshake msg 3 (<- s, se)");
    
    // Extract transport and remote key
    let remote_public_key = extract_remote_public_key(&state)?;
    let transport = state
        .into_transport_mode()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    
    debug!("Handshake complete (responder)");
    Ok(NoiseSession { transport, remote_public_key })
}

fn write_handshake_msg(state: &mut HandshakeState, payload: &[u8]) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; MAX_HANDSHAKE_MSG_SIZE];
    let len = state
        .write_message(payload, &mut buf)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    buf.truncate(len);
    Ok(buf)
}

fn read_handshake_msg(state: &mut HandshakeState, msg: &[u8]) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; MAX_HANDSHAKE_MSG_SIZE];
    let len = state
        .read_message(msg, &mut buf)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    buf.truncate(len);
    Ok(buf)
}

fn extract_remote_public_key(state: &HandshakeState) -> io::Result<[u8; 32]> {
    let key = state
        .get_remote_static()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no remote static key"))?;
    
    let mut result = [0u8; 32];
    result.copy_from_slice(key);
    Ok(result)
}

async fn send_handshake_msg<S: AsyncWriteExt + Unpin>(stream: &mut S, msg: &[u8]) -> io::Result<()> {
    // 2-byte length prefix (big-endian)
    let len = (msg.len() as u16).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(msg).await?;
    stream.flush().await?;
    trace!("Sent {} byte handshake message", msg.len());
    Ok(())
}

async fn recv_handshake_msg<S: AsyncReadExt + Unpin>(stream: &mut S) -> io::Result<Vec<u8>> {
    // 2-byte length prefix
    let mut len_buf = [0u8; 2];
    stream.read_exact(&mut len_buf).await?;
    let len = u16::from_be_bytes(len_buf) as usize;
    
    if len > MAX_HANDSHAKE_MSG_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("handshake message too large: {}", len),
        ));
    }
    
    let mut msg = vec![0u8; len];
    stream.read_exact(&mut msg).await?;
    trace!("Received {} byte handshake message", len);
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, DuplexStream};
    use tokio::net::{TcpListener, TcpStream};
    
    #[test]
    fn test_keypair_generation() {
        let kp = NoiseKeypair::generate();
        
        // Keys should be 32 bytes
        assert_eq!(kp.private.len(), 32);
        assert_eq!(kp.public.len(), 32);
        
        // Keys should not be all zeros
        assert!(kp.private.iter().any(|&b| b != 0));
        assert!(kp.public.iter().any(|&b| b != 0));
    }
    
    #[test]
    fn test_keypair_deterministic() {
        // Different generations should produce different keys
        let kp1 = NoiseKeypair::generate();
        let kp2 = NoiseKeypair::generate();
        
        assert_ne!(kp1.private, kp2.private);
        assert_ne!(kp1.public, kp2.public);
    }
    
    #[tokio::test]
    async fn test_handshake_success() {
        // Generate keypairs
        let initiator_kp = NoiseKeypair::generate();
        let responder_kp = NoiseKeypair::generate();
        
        // Create duplex streams
        let (mut client, mut server) = duplex(1024);
        
        // Run handshakes concurrently
        let initiator_handle = {
            let kp = NoiseKeypair::from_bytes(initiator_kp.private, initiator_kp.public);
            tokio::spawn(async move {
                handshake_initiator(&mut client, &kp).await
            })
        };
        
        let responder_handle = {
            let kp = NoiseKeypair::from_bytes(responder_kp.private, responder_kp.public);
            tokio::spawn(async move {
                handshake_responder(&mut server, &kp).await
            })
        };
        
        // Both should complete successfully
        let initiator_session = initiator_handle.await.unwrap().unwrap();
        let responder_session = responder_handle.await.unwrap().unwrap();
        
        // Each side should have the other's public key
        assert_eq!(initiator_session.remote_public_key(), &responder_kp.public);
        assert_eq!(responder_session.remote_public_key(), &initiator_kp.public);
    }
    
    #[tokio::test]
    async fn test_encrypted_communication() {
        // Set up sessions
        let initiator_kp = NoiseKeypair::generate();
        let responder_kp = NoiseKeypair::generate();
        
        let (mut client, mut server) = duplex(4096);
        
        let initiator_handle = {
            let kp = NoiseKeypair::from_bytes(initiator_kp.private, initiator_kp.public);
            tokio::spawn(async move {
                handshake_initiator(&mut client, &kp).await
            })
        };
        
        let responder_handle = {
            let kp = NoiseKeypair::from_bytes(responder_kp.private, responder_kp.public);
            tokio::spawn(async move {
                handshake_responder(&mut server, &kp).await
            })
        };
        
        let mut initiator_session = initiator_handle.await.unwrap().unwrap();
        let mut responder_session = responder_handle.await.unwrap().unwrap();
        
        // Test encrypt/decrypt both directions
        let msg1 = b"Hello from initiator!";
        let ciphertext1 = initiator_session.encrypt(msg1).unwrap();
        let plaintext1 = responder_session.decrypt(&ciphertext1).unwrap();
        assert_eq!(&plaintext1, msg1);
        
        let msg2 = b"Hello from responder!";
        let ciphertext2 = responder_session.encrypt(msg2).unwrap();
        let plaintext2 = initiator_session.decrypt(&ciphertext2).unwrap();
        assert_eq!(&plaintext2, msg2);
    }
    
    #[tokio::test]
    async fn test_ciphertext_is_authenticated() {
        // Set up sessions
        let initiator_kp = NoiseKeypair::generate();
        let responder_kp = NoiseKeypair::generate();
        
        let (mut client, mut server) = duplex(4096);
        
        let initiator_handle = {
            let kp = NoiseKeypair::from_bytes(initiator_kp.private, initiator_kp.public);
            tokio::spawn(async move { handshake_initiator(&mut client, &kp).await })
        };
        
        let responder_handle = {
            let kp = NoiseKeypair::from_bytes(responder_kp.private, responder_kp.public);
            tokio::spawn(async move { handshake_responder(&mut server, &kp).await })
        };
        
        let mut initiator_session = initiator_handle.await.unwrap().unwrap();
        let mut responder_session = responder_handle.await.unwrap().unwrap();
        
        // Encrypt a message
        let msg = b"Important data";
        let mut ciphertext = initiator_session.encrypt(msg).unwrap();
        
        // Tamper with ciphertext
        if ciphertext.len() > 5 {
            ciphertext[5] ^= 0xFF;
        }
        
        // Decryption should fail
        let result = responder_session.decrypt(&ciphertext);
        assert!(result.is_err(), "Tampered ciphertext should fail to decrypt");
    }
    
    #[tokio::test]
    async fn test_handshake_over_tcp() {
        // Start a TCP server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let initiator_kp = NoiseKeypair::generate();
        let responder_kp = NoiseKeypair::generate();
        
        // Server task
        let server_handle = {
            let kp = NoiseKeypair::from_bytes(responder_kp.private, responder_kp.public);
            tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                handshake_responder(&mut stream, &kp).await
            })
        };
        
        // Client task
        let client_handle = {
            let kp = NoiseKeypair::from_bytes(initiator_kp.private, initiator_kp.public);
            tokio::spawn(async move {
                let mut stream = TcpStream::connect(addr).await.unwrap();
                handshake_initiator(&mut stream, &kp).await
            })
        };
        
        // Both should complete
        let client_session = client_handle.await.unwrap().unwrap();
        let server_session = server_handle.await.unwrap().unwrap();
        
        assert_eq!(client_session.remote_public_key(), &responder_kp.public);
        assert_eq!(server_session.remote_public_key(), &initiator_kp.public);
    }
}

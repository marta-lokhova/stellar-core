//! Stellar message framing codec.
//!
//! Wire format: 4-byte big-endian length prefix + payload
//! This matches the XDR framing used by stellar-core.

use std::io::{self, Read, Write};

/// Maximum message size (256 KB, matching stellar-core's MAX_MESSAGE_SIZE)
pub const MAX_MESSAGE_SIZE: usize = 256 * 1024;

/// Header size (4 bytes for length)
const HEADER_SIZE: usize = 4;

/// Read a length-prefixed message from a stream.
///
/// Format: [length:4 bytes big-endian][payload:length bytes]
pub fn read_message<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    // Read 4-byte length prefix (big-endian)
    let mut header = [0u8; HEADER_SIZE];
    reader.read_exact(&mut header)?;
    
    // XDR uses big-endian, and the MSB bit is a "continuation" flag that should be cleared
    let length = u32::from_be_bytes(header) & 0x7FFF_FFFF;
    let length = length as usize;
    
    // Sanity check
    if length > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("message too large: {} bytes (max {})", length, MAX_MESSAGE_SIZE),
        ));
    }
    
    // Read payload
    let mut payload = vec![0u8; length];
    if length > 0 {
        reader.read_exact(&mut payload)?;
    }
    
    Ok(payload)
}

/// Write a length-prefixed message to a stream.
///
/// Format: [length:4 bytes big-endian][payload:length bytes]
pub fn write_message<W: Write>(writer: &mut W, payload: &[u8]) -> io::Result<()> {
    if payload.len() > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("message too large: {} bytes (max {})", payload.len(), MAX_MESSAGE_SIZE),
        ));
    }
    
    // Write 4-byte length prefix (big-endian)
    // Set the MSB to indicate this is the final fragment (XDR record marking)
    let length = (payload.len() as u32) | 0x8000_0000;
    writer.write_all(&length.to_be_bytes())?;
    
    // Write payload
    if !payload.is_empty() {
        writer.write_all(payload)?;
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    
    #[test]
    fn test_write_read_roundtrip() {
        let payload = vec![1, 2, 3, 4, 5];
        
        let mut buf = Vec::new();
        write_message(&mut buf, &payload).unwrap();
        
        let mut cursor = Cursor::new(buf);
        let decoded = read_message(&mut cursor).unwrap();
        
        assert_eq!(decoded, payload);
    }
    
    #[test]
    fn test_empty_message() {
        let payload = vec![];
        
        let mut buf = Vec::new();
        write_message(&mut buf, &payload).unwrap();
        
        // Should be exactly 4 bytes (just the header)
        assert_eq!(buf.len(), 4);
        
        let mut cursor = Cursor::new(buf);
        let decoded = read_message(&mut cursor).unwrap();
        
        assert!(decoded.is_empty());
    }
    
    #[test]
    fn test_big_endian_format() {
        let payload = vec![0xAA, 0xBB];
        
        let mut buf = Vec::new();
        write_message(&mut buf, &payload).unwrap();
        
        // Header should be: 0x80 0x00 0x00 0x02 (MSB set + length 2 in big-endian)
        assert_eq!(buf[0], 0x80);
        assert_eq!(buf[1], 0x00);
        assert_eq!(buf[2], 0x00);
        assert_eq!(buf[3], 0x02);
        
        // Payload follows
        assert_eq!(buf[4], 0xAA);
        assert_eq!(buf[5], 0xBB);
    }
    
    #[test]
    fn test_read_clears_continuation_bit() {
        // Manually craft a message with MSB set (as stellar-core sends)
        let mut buf = vec![0x80, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03];
        
        let mut cursor = Cursor::new(&mut buf);
        let decoded = read_message(&mut cursor).unwrap();
        
        assert_eq!(decoded, vec![1, 2, 3]);
    }
    
    #[test]
    fn test_read_without_continuation_bit() {
        // Message without MSB set (shouldn't happen, but handle gracefully)
        let mut buf = vec![0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03];
        
        let mut cursor = Cursor::new(&mut buf);
        let decoded = read_message(&mut cursor).unwrap();
        
        assert_eq!(decoded, vec![1, 2, 3]);
    }
    
    #[test]
    fn test_reject_oversized_message() {
        // Try to write a message that's too large
        let payload = vec![0u8; MAX_MESSAGE_SIZE + 1];
        
        let mut buf = Vec::new();
        let result = write_message(&mut buf, &payload);
        
        assert!(result.is_err());
    }
    
    #[test]
    fn test_reject_oversized_read() {
        // Craft a header claiming a huge message
        let buf = vec![0x80, 0x10, 0x00, 0x00]; // Claims ~1MB
        
        let mut cursor = Cursor::new(buf);
        let result = read_message(&mut cursor);
        
        assert!(result.is_err());
    }
    
    #[test]
    fn test_large_valid_message() {
        // Test with a message near the size limit
        let payload = vec![0x42u8; 100_000];
        
        let mut buf = Vec::new();
        write_message(&mut buf, &payload).unwrap();
        
        let mut cursor = Cursor::new(buf);
        let decoded = read_message(&mut cursor).unwrap();
        
        assert_eq!(decoded.len(), 100_000);
        assert!(decoded.iter().all(|&b| b == 0x42));
    }
}

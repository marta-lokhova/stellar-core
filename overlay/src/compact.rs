//! BIP152-style compact tx set dissemination: wire types and codecs.
//!
//! A nomination candidate travels as a `CompactTxSet` — the tx set hash plus a
//! 6-byte SipHash-2-4 short id per transaction (keyed by the tx set hash) —
//! instead of the full multi-MB `GeneralizedTxSet`. Receivers match short ids
//! against their mempool, request only the missing transactions
//! (`CompactTxSetGetTxs` / `CompactTxSetTxs`), reconstruct the full XDR and
//! verify its SHA-256 before handing it to core.
//!
//! The XDR definitions mirror the `CompactTxSet*` types from the
//! proto-compact-blocks-soroban prototype (drebelsky/rs-stellar-xdr@9e499d3e):
//!
//! ```text
//! enum CompactTxSetMessageType
//! {
//!     COMPACT_TX_SET = 0,
//!     COMPACT_TX_SET_GET = 1,
//!     COMPACT_TX_SET_GET_TXS = 2,
//!     COMPACT_TX_SET_TXS = 3
//! };
//!
//! struct CompactTxSet
//! {
//!     Hash txSetHash; // hash of the full tx set
//!     Hash previousLedgerHash;
//!     int64* baseFee;
//!     uint32 numSorobanTxs;
//!     int64* sorobanBaseFee;
//!     // 6 byte siphashes
//!     opaque txs<>;
//! };
//!
//! struct CompactTxSetGet { Hash txSetHash; };
//!
//! struct CompactTxSetGetTxs
//! {
//!     Hash txSetHash;
//!     // differentially encoded indices of transactions requested
//!     opaque indices<>;
//! };
//!
//! struct CompactTxSetTxs
//! {
//!     Hash txSetHash;
//!     TransactionEnvelope txs<>;
//! };
//!
//! union CompactTxSetMessage switch (CompactTxSetMessageType type)
//! {
//! case COMPACT_TX_SET:         CompactTxSet compactTxSet;
//! case COMPACT_TX_SET_GET:     CompactTxSetGet compactTxSetGet;
//! case COMPACT_TX_SET_GET_TXS: CompactTxSetGetTxs compactTxSetGetTxs;
//! case COMPACT_TX_SET_TXS:     CompactTxSetTxs compactTxSetTxs;
//! };
//! ```
//!
//! They are hand-written against the crates.io stellar-xdr (same wire format
//! as the fork's generated code) so the overlay doesn't need a forked XDR
//! crate; both sides of every compact exchange run this binary.

use siphasher::sip::SipHasher24;
use std::fmt;
use std::hash::Hasher;
use std::io::{Read, Write};
use stellar_xdr::curr::{
    BytesM, Error, Hash, Limited, Limits, ReadXdr, TransactionEnvelope, VecM, WriteXdr,
};

/// Length of a short transaction id (bytes).
pub const SHORT_ID_LEN: usize = 6;

/// Decode limits for compact messages arriving from untrusted peers:
/// depth-bounded so a hostile nested `TransactionEnvelope` can't overflow the
/// stack.
pub fn decode_limits() -> Limits {
    Limits::depth(500)
}

/// Compute the 6-byte short id for a transaction: SipHash-2-4 of the tx
/// content hash, keyed by the first 16 bytes of the tx set hash, taking bytes
/// [2..8] of the big-endian digest.
pub fn short_tx_id(key: &[u8; 16], tx_hash: &[u8; 32]) -> [u8; SHORT_ID_LEN] {
    let mut hasher = SipHasher24::new_with_key(key);
    hasher.write(tx_hash);
    let digest = hasher.finish().to_be_bytes();
    let mut short = [0u8; SHORT_ID_LEN];
    short.copy_from_slice(&digest[2..8]);
    short
}

/// `CompactTxSetMessageType` XDR enum.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactTxSetMessageType {
    Set = 0,
    SetGet = 1,
    SetGetTxs = 2,
    SetTxs = 3,
}

/// `CompactTxSet` XDR struct.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactTxSet {
    pub tx_set_hash: Hash,
    pub previous_ledger_hash: Hash,
    pub base_fee: Option<i64>,
    pub num_soroban_txs: u32,
    pub soroban_base_fee: Option<i64>,
    /// Concatenated 6-byte short ids, in tx set order.
    pub txs: BytesM,
}

/// `CompactTxSetGet` XDR struct.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactTxSetGet {
    pub tx_set_hash: Hash,
}

/// `CompactTxSetGetTxs` XDR struct.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactTxSetGetTxs {
    pub tx_set_hash: Hash,
    /// Differentially varint-encoded indices of the requested transactions.
    pub indices: BytesM,
}

/// `CompactTxSetTxs` XDR struct.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactTxSetTxs {
    pub tx_set_hash: Hash,
    pub txs: VecM<TransactionEnvelope>,
}

/// `CompactTxSetMessage` XDR union.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactTxSetMessage {
    Set(CompactTxSet),
    SetGet(CompactTxSetGet),
    SetGetTxs(CompactTxSetGetTxs),
    SetTxs(CompactTxSetTxs),
}

impl ReadXdr for CompactTxSet {
    fn read_xdr<R: Read>(r: &mut Limited<R>) -> Result<Self, Error> {
        Ok(Self {
            tx_set_hash: Hash::read_xdr(r)?,
            previous_ledger_hash: Hash::read_xdr(r)?,
            base_fee: Option::<i64>::read_xdr(r)?,
            num_soroban_txs: u32::read_xdr(r)?,
            soroban_base_fee: Option::<i64>::read_xdr(r)?,
            txs: BytesM::read_xdr(r)?,
        })
    }
}

impl WriteXdr for CompactTxSet {
    fn write_xdr<W: Write>(&self, w: &mut Limited<W>) -> Result<(), Error> {
        self.tx_set_hash.write_xdr(w)?;
        self.previous_ledger_hash.write_xdr(w)?;
        self.base_fee.write_xdr(w)?;
        self.num_soroban_txs.write_xdr(w)?;
        self.soroban_base_fee.write_xdr(w)?;
        self.txs.write_xdr(w)?;
        Ok(())
    }
}

impl ReadXdr for CompactTxSetGet {
    fn read_xdr<R: Read>(r: &mut Limited<R>) -> Result<Self, Error> {
        Ok(Self {
            tx_set_hash: Hash::read_xdr(r)?,
        })
    }
}

impl WriteXdr for CompactTxSetGet {
    fn write_xdr<W: Write>(&self, w: &mut Limited<W>) -> Result<(), Error> {
        self.tx_set_hash.write_xdr(w)
    }
}

impl ReadXdr for CompactTxSetGetTxs {
    fn read_xdr<R: Read>(r: &mut Limited<R>) -> Result<Self, Error> {
        Ok(Self {
            tx_set_hash: Hash::read_xdr(r)?,
            indices: BytesM::read_xdr(r)?,
        })
    }
}

impl WriteXdr for CompactTxSetGetTxs {
    fn write_xdr<W: Write>(&self, w: &mut Limited<W>) -> Result<(), Error> {
        self.tx_set_hash.write_xdr(w)?;
        self.indices.write_xdr(w)?;
        Ok(())
    }
}

impl ReadXdr for CompactTxSetTxs {
    fn read_xdr<R: Read>(r: &mut Limited<R>) -> Result<Self, Error> {
        Ok(Self {
            tx_set_hash: Hash::read_xdr(r)?,
            txs: VecM::<TransactionEnvelope>::read_xdr(r)?,
        })
    }
}

impl WriteXdr for CompactTxSetTxs {
    fn write_xdr<W: Write>(&self, w: &mut Limited<W>) -> Result<(), Error> {
        self.tx_set_hash.write_xdr(w)?;
        self.txs.write_xdr(w)?;
        Ok(())
    }
}

impl ReadXdr for CompactTxSetMessage {
    fn read_xdr<R: Read>(r: &mut Limited<R>) -> Result<Self, Error> {
        let discriminant = u32::read_xdr(r)?;
        match discriminant {
            0 => Ok(Self::Set(CompactTxSet::read_xdr(r)?)),
            1 => Ok(Self::SetGet(CompactTxSetGet::read_xdr(r)?)),
            2 => Ok(Self::SetGetTxs(CompactTxSetGetTxs::read_xdr(r)?)),
            3 => Ok(Self::SetTxs(CompactTxSetTxs::read_xdr(r)?)),
            _ => Err(Error::Invalid),
        }
    }
}

impl WriteXdr for CompactTxSetMessage {
    fn write_xdr<W: Write>(&self, w: &mut Limited<W>) -> Result<(), Error> {
        match self {
            Self::Set(v) => {
                (CompactTxSetMessageType::Set as u32).write_xdr(w)?;
                v.write_xdr(w)
            }
            Self::SetGet(v) => {
                (CompactTxSetMessageType::SetGet as u32).write_xdr(w)?;
                v.write_xdr(w)
            }
            Self::SetGetTxs(v) => {
                (CompactTxSetMessageType::SetGetTxs as u32).write_xdr(w)?;
                v.write_xdr(w)
            }
            Self::SetTxs(v) => {
                (CompactTxSetMessageType::SetTxs as u32).write_xdr(w)?;
                v.write_xdr(w)
            }
        }
    }
}

/// Errors from the differential-index codec (untrusted input; never panic).
#[derive(Debug, PartialEq, Eq)]
pub enum CompactError {
    /// Varint stream ended mid-value or used an unsupported width.
    MalformedVarint,
    /// Decoded index stream would overflow or is out of order.
    InvalidIndices,
}

impl fmt::Display for CompactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompactError::MalformedVarint => write!(f, "malformed varint in compact indices"),
            CompactError::InvalidIndices => write!(f, "invalid compact index sequence"),
        }
    }
}

impl std::error::Error for CompactError {}

/// Append a Bitcoin-style varint (1, 3, or 5 bytes; 8-byte form is never
/// needed for tx set indices).
pub fn encode_varint(value: usize, buffer: &mut Vec<u8>) {
    if value < 0xFD {
        buffer.push(value as u8);
    } else if value <= 0xFFFF {
        buffer.push(0xFD);
        buffer.extend_from_slice(&(value as u16).to_le_bytes());
    } else if value <= 0xFFFF_FFFF {
        buffer.push(0xFE);
        buffer.extend_from_slice(&(value as u32).to_le_bytes());
    } else {
        // A tx set can never hold 2^32 transactions.
        unreachable!("index too large for varint encoding");
    }
}

fn decode_varint(data: &[u8], offset: &mut usize) -> Result<usize, CompactError> {
    let first_byte = *data.get(*offset).ok_or(CompactError::MalformedVarint)?;
    *offset += 1;

    match first_byte {
        0xFD => {
            let bytes = data
                .get(*offset..*offset + 2)
                .ok_or(CompactError::MalformedVarint)?;
            *offset += 2;
            Ok(u16::from_le_bytes(bytes.try_into().unwrap()) as usize)
        }
        0xFE => {
            let bytes = data
                .get(*offset..*offset + 4)
                .ok_or(CompactError::MalformedVarint)?;
            *offset += 4;
            Ok(u32::from_le_bytes(bytes.try_into().unwrap()) as usize)
        }
        0xFF => Err(CompactError::MalformedVarint),
        _ => Ok(first_byte as usize),
    }
}

/// Encode a strictly-increasing index list as varint gaps: the first value
/// verbatim, then each subsequent value as (gap from previous - 1).
/// E.g. `[1, 2, 10]` encodes as varints `[1, 0, 7]`.
pub fn create_differential_indices(indices: &[usize]) -> Vec<u8> {
    let mut result = Vec::new();
    let Some((&first, rest)) = indices.split_first() else {
        return result;
    };
    encode_varint(first, &mut result);
    let mut prev_index = first;
    for &index in rest {
        encode_varint(index - prev_index - 1, &mut result);
        prev_index = index;
    }
    result
}

/// Decode a differential varint index stream back to absolute indices.
pub fn parse_differential_indices(indices: &[u8]) -> Result<Vec<usize>, CompactError> {
    let mut offset = 0;
    let mut result = Vec::new();
    if indices.is_empty() {
        return Ok(result);
    }
    let mut current_index = decode_varint(indices, &mut offset)?;
    result.push(current_index);
    while offset < indices.len() {
        let gap = decode_varint(indices, &mut offset)?;
        current_index = current_index
            .checked_add(gap)
            .and_then(|v| v.checked_add(1))
            .ok_or(CompactError::InvalidIndices)?;
        result.push(current_index);
    }
    Ok(result)
}

/// Build the wire bytes for a `CompactTxSetMessage::SetTxs` reply directly
/// from already-serialized transaction XDR, avoiding a decode + typed
/// re-encode of every envelope. Equivalence with the typed encoding is pinned
/// by `set_txs_manual_framing_matches_typed` below.
pub fn build_set_txs_message<'a>(
    tx_set_hash: &[u8; 32],
    txs: impl ExactSizeIterator<Item = &'a [u8]>,
) -> Vec<u8> {
    let mut msg: Vec<u8> = Vec::new();
    msg.extend_from_slice(&(CompactTxSetMessageType::SetTxs as u32).to_be_bytes());
    msg.extend_from_slice(tx_set_hash);
    msg.extend_from_slice(&(txs.len() as u32).to_be_bytes());
    for tx in txs {
        msg.extend_from_slice(tx);
    }
    msg
}

#[cfg(test)]
mod tests {
    use super::*;
    use stellar_xdr::curr::VecM;

    fn sample_compact_set() -> CompactTxSet {
        CompactTxSet {
            tx_set_hash: Hash([0x11; 32]),
            previous_ledger_hash: Hash([0x22; 32]),
            base_fee: Some(100),
            num_soroban_txs: 3,
            soroban_base_fee: None,
            txs: BytesM::try_from(vec![0xAB; SHORT_ID_LEN * 5]).unwrap(),
        }
    }

    #[test]
    fn compact_message_roundtrip() {
        let messages = vec![
            CompactTxSetMessage::Set(sample_compact_set()),
            CompactTxSetMessage::SetGet(CompactTxSetGet {
                tx_set_hash: Hash([0x33; 32]),
            }),
            CompactTxSetMessage::SetGetTxs(CompactTxSetGetTxs {
                tx_set_hash: Hash([0x44; 32]),
                indices: BytesM::try_from(create_differential_indices(&[1, 2, 10])).unwrap(),
            }),
            CompactTxSetMessage::SetTxs(CompactTxSetTxs {
                tx_set_hash: Hash([0x55; 32]),
                txs: VecM::try_from(vec![TransactionEnvelope::default()]).unwrap(),
            }),
        ];
        for msg in messages {
            let bytes = msg.to_xdr(Limits::none()).unwrap();
            let decoded = CompactTxSetMessage::from_xdr(&bytes, decode_limits()).unwrap();
            assert_eq!(msg, decoded);
        }
    }

    #[test]
    fn unknown_discriminant_is_rejected() {
        let mut bytes = 7u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0u8; 32]);
        assert!(CompactTxSetMessage::from_xdr(&bytes, decode_limits()).is_err());
    }

    #[test]
    fn differential_indices_roundtrip() {
        let cases: Vec<Vec<usize>> = vec![
            vec![],
            vec![0],
            vec![1, 2, 10],
            vec![0, 1, 2, 3],
            vec![250, 260, 70_000, 70_001, 5_000_000],
        ];
        for indices in cases {
            let encoded = create_differential_indices(&indices);
            let decoded = parse_differential_indices(&encoded).unwrap();
            assert_eq!(indices, decoded);
        }
    }

    #[test]
    fn differential_indices_example_encoding() {
        // [1, 2, 10] -> gaps [1, 0, 7]
        assert_eq!(create_differential_indices(&[1, 2, 10]), vec![1, 0, 7]);
    }

    #[test]
    fn truncated_varint_is_rejected() {
        // 0xFD announces a 2-byte value but only 1 byte follows
        assert_eq!(
            parse_differential_indices(&[0xFD, 0x01]),
            Err(CompactError::MalformedVarint)
        );
        // 8-byte form is unsupported
        assert_eq!(
            parse_differential_indices(&[0xFF]),
            Err(CompactError::MalformedVarint)
        );
    }

    #[test]
    fn set_txs_manual_framing_matches_typed() {
        let env = TransactionEnvelope::default();
        let env_xdr = env.to_xdr(Limits::none()).unwrap();
        let hash = [0x66; 32];

        let typed = CompactTxSetMessage::SetTxs(CompactTxSetTxs {
            tx_set_hash: Hash(hash),
            txs: VecM::try_from(vec![env.clone(), env]).unwrap(),
        })
        .to_xdr(Limits::none())
        .unwrap();

        let manual =
            build_set_txs_message(&hash, [env_xdr.as_slice(), env_xdr.as_slice()].into_iter());
        assert_eq!(typed, manual);
    }

    #[test]
    fn short_id_is_stable() {
        let key = [0x01; 16];
        let tx_hash = [0x02; 32];
        let a = short_tx_id(&key, &tx_hash);
        let b = short_tx_id(&key, &tx_hash);
        assert_eq!(a, b);
        // Different key -> different id (with overwhelming probability)
        let c = short_tx_id(&[0x03; 16], &tx_hash);
        assert_ne!(a, c);
    }
}

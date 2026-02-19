//! Minimal XDR parser for TransactionEnvelope.
//!
//! Extracts fee and operation count from a raw TransactionEnvelope XDR blob
//! without needing a full XDR library. Only parses the fields we need.

/// Extracted transaction metadata from XDR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxMetadata {
    /// Fee in stroops
    pub fee: u64,
    /// Number of operations
    pub num_ops: u32,
}

/// Parse fee and operation count from a TransactionEnvelope XDR blob.
///
/// Supports ENVELOPE_TYPE_TX_V0 (0), ENVELOPE_TYPE_TX (2), and
/// ENVELOPE_TYPE_TX_FEE_BUMP (5).
///
/// Returns None if the XDR is malformed or too short.
pub fn parse_tx_metadata(data: &[u8]) -> Option<TxMetadata> {
    let mut cursor = Cursor::new(data);

    let envelope_type = cursor.read_u32()?;
    match envelope_type {
        // ENVELOPE_TYPE_TX_V0
        0 => parse_tx_v0_metadata(&mut cursor),
        // ENVELOPE_TYPE_TX
        2 => parse_tx_v1_metadata(&mut cursor),
        // ENVELOPE_TYPE_TX_FEE_BUMP
        5 => parse_fee_bump_metadata(&mut cursor),
        _ => None,
    }
}

/// TransactionV0: sourceAccountEd25519 (32 bytes fixed) then fee (u32)
fn parse_tx_v0_metadata(cursor: &mut Cursor) -> Option<TxMetadata> {
    // Skip sourceAccountEd25519: opaque[32]
    cursor.skip(32)?;
    let fee = cursor.read_u32()? as u64;
    // Skip seqNum: int64
    cursor.skip(8)?;
    // Skip timeBounds: optional (union with bool discriminant)
    skip_optional_time_bounds(cursor)?;
    // Skip memo
    skip_memo(cursor)?;
    // operations<MAX_OPS_PER_TX> - array with length prefix
    let num_ops = cursor.read_u32()?;
    Some(TxMetadata { fee, num_ops })
}

/// Transaction (v1): MuxedAccount (variable) then fee (u32)
fn parse_tx_v1_metadata(cursor: &mut Cursor) -> Option<TxMetadata> {
    // Skip sourceAccount: MuxedAccount (variable length)
    skip_muxed_account(cursor)?;
    let fee = cursor.read_u32()? as u64;
    // Skip seqNum: int64
    cursor.skip(8)?;
    // Skip Preconditions (more complex than TimeBounds)
    skip_preconditions(cursor)?;
    // Skip memo
    skip_memo(cursor)?;
    // operations<MAX_OPS_PER_TX>
    let num_ops = cursor.read_u32()?;
    Some(TxMetadata { fee, num_ops })
}

/// FeeBumpTransaction: MuxedAccount feeSource, int64 fee, innerTx
fn parse_fee_bump_metadata(cursor: &mut Cursor) -> Option<TxMetadata> {
    // Skip feeSource: MuxedAccount
    skip_muxed_account(cursor)?;
    // fee: int64 (use as fee for the bump)
    let fee = cursor.read_i64()? as u64;
    // innerTx: TransactionEnvelope (we need op count from inner)
    // innerTx discriminant: ENVELOPE_TYPE_TX (2)
    let inner_type = cursor.read_u32()?;
    if inner_type != 2 {
        // V0 inner not expected in fee bump, but try
        return None;
    }
    // Parse inner Transaction to get num_ops
    let inner = parse_tx_v1_metadata(cursor)?;
    Some(TxMetadata {
        fee,
        num_ops: inner.num_ops,
    })
}

/// Skip a MuxedAccount union.
/// KEY_TYPE_ED25519 (0): 32 bytes
/// KEY_TYPE_MUXED_ED25519 (256): 8 + 32 = 40 bytes
fn skip_muxed_account(cursor: &mut Cursor) -> Option<()> {
    let account_type = cursor.read_u32()?;
    match account_type {
        0 => cursor.skip(32),             // ed25519 public key
        256 => cursor.skip(8 + 32),       // id (u64) + ed25519
        _ => None,
    }
}

/// Skip optional TimeBounds (used in TX_V0).
/// XDR optional = bool (u32), if true then the value follows.
fn skip_optional_time_bounds(cursor: &mut Cursor) -> Option<()> {
    let has_time_bounds = cursor.read_u32()?;
    if has_time_bounds != 0 {
        // TimeBounds: minTime (u64) + maxTime (u64) = 16 bytes
        cursor.skip(16)?;
    }
    Some(())
}

/// Skip Preconditions union (used in TX v1).
/// PreconditionType: NONE(0), TIME(1), V2(2)
fn skip_preconditions(cursor: &mut Cursor) -> Option<()> {
    let precond_type = cursor.read_u32()?;
    match precond_type {
        0 => Some(()),  // PRECOND_NONE
        1 => {
            // PRECOND_TIME: TimeBounds { minTime: u64, maxTime: u64 }
            cursor.skip(16)
        }
        2 => {
            // PRECOND_V2: complex, skip TimeBounds + LedgerBounds + ...
            // TimeBounds (optional)
            skip_optional_time_bounds(cursor)?;
            // LedgerBounds (optional)
            let has_ledger_bounds = cursor.read_u32()?;
            if has_ledger_bounds != 0 {
                cursor.skip(8)?; // minLedger + maxLedger (u32 + u32)
            }
            // minSeqNum (optional int64)
            let has_min_seq = cursor.read_u32()?;
            if has_min_seq != 0 {
                cursor.skip(8)?;
            }
            // minSeqAge: Duration (u64)
            cursor.skip(8)?;
            // minSeqLedgerGap: uint32
            cursor.skip(4)?;
            // extraSigners<2>: variable length array of SignerKey
            let num_signers = cursor.read_u32()?;
            for _ in 0..num_signers {
                skip_signer_key(cursor)?;
            }
            Some(())
        }
        _ => None,
    }
}

/// Skip a SignerKey union.
fn skip_signer_key(cursor: &mut Cursor) -> Option<()> {
    let key_type = cursor.read_u32()?;
    match key_type {
        0 | 1 | 2 => cursor.skip(32),    // ed25519, preAuthTx, hashX: all 32 bytes
        3 => {
            // ed25519SignedPayload: ed25519 (32) + payload<64>
            cursor.skip(32)?;
            let payload_len = cursor.read_u32()?;
            let padded = (payload_len + 3) & !3; // XDR pads to 4 bytes
            cursor.skip(padded as usize)
        }
        _ => None,
    }
}

/// Skip a Memo union.
fn skip_memo(cursor: &mut Cursor) -> Option<()> {
    let memo_type = cursor.read_u32()?;
    match memo_type {
        0 => Some(()),                    // MEMO_NONE
        1 => {
            // MEMO_TEXT: string<28>
            let len = cursor.read_u32()?;
            let padded = (len + 3) & !3;
            cursor.skip(padded as usize)
        }
        2 => cursor.skip(8),             // MEMO_ID: uint64
        3 | 4 => cursor.skip(32),        // MEMO_HASH / MEMO_RETURN: opaque[32]
        _ => None,
    }
}

// ─── Cursor helper ───

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Cursor { data, pos: 0 }
    }

    fn read_u32(&mut self) -> Option<u32> {
        if self.pos + 4 > self.data.len() {
            return None;
        }
        let val = u32::from_be_bytes(self.data[self.pos..self.pos + 4].try_into().ok()?);
        self.pos += 4;
        Some(val)
    }

    fn read_i64(&mut self) -> Option<i64> {
        if self.pos + 8 > self.data.len() {
            return None;
        }
        let val = i64::from_be_bytes(self.data[self.pos..self.pos + 8].try_into().ok()?);
        self.pos += 8;
        Some(val)
    }

    fn skip(&mut self, n: usize) -> Option<()> {
        if self.pos + n > self.data.len() {
            return None;
        }
        self.pos += n;
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal ENVELOPE_TYPE_TX_V0 envelope.
    /// Format: [type:u32=0][sourceEd25519:32][fee:u32][seqNum:i64]
    ///         [hasTimeBounds:u32=0][memoType:u32=0][numOps:u32]
    fn build_tx_v0(fee: u32, num_ops: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_be_bytes());         // ENVELOPE_TYPE_TX_V0
        buf.extend_from_slice(&[0u8; 32]);                   // sourceAccountEd25519
        buf.extend_from_slice(&fee.to_be_bytes());           // fee
        buf.extend_from_slice(&1i64.to_be_bytes());          // seqNum
        buf.extend_from_slice(&0u32.to_be_bytes());          // hasTimeBounds = false
        buf.extend_from_slice(&0u32.to_be_bytes());          // memo = MEMO_NONE
        buf.extend_from_slice(&num_ops.to_be_bytes());       // numOps
        buf
    }

    /// Build a minimal ENVELOPE_TYPE_TX (v1) envelope with ed25519 source.
    /// Format: [type:u32=2][accountType:u32=0][ed25519:32][fee:u32][seqNum:i64]
    ///         [precondType:u32=0][memoType:u32=0][numOps:u32]
    fn build_tx_v1(fee: u32, num_ops: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_be_bytes());          // ENVELOPE_TYPE_TX
        buf.extend_from_slice(&0u32.to_be_bytes());          // KEY_TYPE_ED25519
        buf.extend_from_slice(&[0xAB; 32]);                  // ed25519 public key
        buf.extend_from_slice(&fee.to_be_bytes());           // fee
        buf.extend_from_slice(&42i64.to_be_bytes());         // seqNum
        buf.extend_from_slice(&0u32.to_be_bytes());          // PRECOND_NONE
        buf.extend_from_slice(&0u32.to_be_bytes());          // MEMO_NONE
        buf.extend_from_slice(&num_ops.to_be_bytes());       // numOps
        buf
    }

    /// Build ENVELOPE_TYPE_TX with MuxedAccount (KEY_TYPE_MUXED_ED25519=256).
    fn build_tx_v1_muxed(fee: u32, num_ops: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_be_bytes());          // ENVELOPE_TYPE_TX
        buf.extend_from_slice(&256u32.to_be_bytes());        // KEY_TYPE_MUXED_ED25519
        buf.extend_from_slice(&99u64.to_be_bytes());         // muxed id
        buf.extend_from_slice(&[0xCD; 32]);                  // ed25519 key
        buf.extend_from_slice(&fee.to_be_bytes());           // fee
        buf.extend_from_slice(&1i64.to_be_bytes());          // seqNum
        buf.extend_from_slice(&0u32.to_be_bytes());          // PRECOND_NONE
        buf.extend_from_slice(&0u32.to_be_bytes());          // MEMO_NONE
        buf.extend_from_slice(&num_ops.to_be_bytes());       // numOps
        buf
    }

    /// Build ENVELOPE_TYPE_TX_FEE_BUMP wrapping a v1 inner TX.
    fn build_fee_bump(outer_fee: i64, inner_fee: u32, num_ops: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&5u32.to_be_bytes());          // ENVELOPE_TYPE_TX_FEE_BUMP
        buf.extend_from_slice(&0u32.to_be_bytes());          // feeSource: KEY_TYPE_ED25519
        buf.extend_from_slice(&[0xEF; 32]);                  // ed25519 key
        buf.extend_from_slice(&outer_fee.to_be_bytes());     // fee (i64)
        // innerTx: ENVELOPE_TYPE_TX (discriminant only, no outer envelope type)
        buf.extend_from_slice(&2u32.to_be_bytes());          // inner envelope type
        buf.extend_from_slice(&0u32.to_be_bytes());          // inner source: KEY_TYPE_ED25519
        buf.extend_from_slice(&[0x11; 32]);                  // inner ed25519 key
        buf.extend_from_slice(&inner_fee.to_be_bytes());     // inner fee
        buf.extend_from_slice(&1i64.to_be_bytes());          // inner seqNum
        buf.extend_from_slice(&0u32.to_be_bytes());          // inner PRECOND_NONE
        buf.extend_from_slice(&0u32.to_be_bytes());          // inner MEMO_NONE
        buf.extend_from_slice(&num_ops.to_be_bytes());       // inner numOps
        buf
    }

    // ─── RED tests: verify the parser extracts fee and ops correctly ───

    #[test]
    fn test_parse_tx_v0_basic() {
        let data = build_tx_v0(1000, 3);
        let meta = parse_tx_metadata(&data).expect("should parse v0");
        assert_eq!(meta.fee, 1000);
        assert_eq!(meta.num_ops, 3);
    }

    #[test]
    fn test_parse_tx_v1_basic() {
        let data = build_tx_v1(5000, 2);
        let meta = parse_tx_metadata(&data).expect("should parse v1");
        assert_eq!(meta.fee, 5000);
        assert_eq!(meta.num_ops, 2);
    }

    #[test]
    fn test_parse_tx_v1_muxed_account() {
        let data = build_tx_v1_muxed(7500, 4);
        let meta = parse_tx_metadata(&data).expect("should parse v1 muxed");
        assert_eq!(meta.fee, 7500);
        assert_eq!(meta.num_ops, 4);
    }

    #[test]
    fn test_parse_fee_bump() {
        let data = build_fee_bump(20000, 100, 1);
        let meta = parse_tx_metadata(&data).expect("should parse fee bump");
        // Fee bump uses the OUTER fee
        assert_eq!(meta.fee, 20000);
        // Op count comes from inner TX
        assert_eq!(meta.num_ops, 1);
    }

    #[test]
    fn test_parse_empty_data_returns_none() {
        assert_eq!(parse_tx_metadata(&[]), None);
    }

    #[test]
    fn test_parse_truncated_data_returns_none() {
        // Just the envelope type, nothing else
        assert_eq!(parse_tx_metadata(&[0, 0, 0, 2]), None);
    }

    #[test]
    fn test_parse_unknown_envelope_type_returns_none() {
        let mut data = build_tx_v1(100, 1);
        // Corrupt envelope type to 99
        data[0..4].copy_from_slice(&99u32.to_be_bytes());
        assert_eq!(parse_tx_metadata(&data), None);
    }

    #[test]
    fn test_parse_tx_v1_with_time_precondition() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_be_bytes());          // ENVELOPE_TYPE_TX
        buf.extend_from_slice(&0u32.to_be_bytes());          // KEY_TYPE_ED25519
        buf.extend_from_slice(&[0xAB; 32]);                  // ed25519
        buf.extend_from_slice(&3000u32.to_be_bytes());       // fee
        buf.extend_from_slice(&1i64.to_be_bytes());          // seqNum
        buf.extend_from_slice(&1u32.to_be_bytes());          // PRECOND_TIME
        buf.extend_from_slice(&0u64.to_be_bytes());          // minTime
        buf.extend_from_slice(&u64::MAX.to_be_bytes());      // maxTime
        buf.extend_from_slice(&0u32.to_be_bytes());          // MEMO_NONE
        buf.extend_from_slice(&5u32.to_be_bytes());          // numOps

        let meta = parse_tx_metadata(&buf).expect("should parse time precond");
        assert_eq!(meta.fee, 3000);
        assert_eq!(meta.num_ops, 5);
    }

    #[test]
    fn test_parse_tx_v0_with_time_bounds() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_be_bytes());          // ENVELOPE_TYPE_TX_V0
        buf.extend_from_slice(&[0u8; 32]);                   // sourceEd25519
        buf.extend_from_slice(&200u32.to_be_bytes());        // fee
        buf.extend_from_slice(&1i64.to_be_bytes());          // seqNum
        buf.extend_from_slice(&1u32.to_be_bytes());          // hasTimeBounds = true
        buf.extend_from_slice(&100u64.to_be_bytes());        // minTime
        buf.extend_from_slice(&200u64.to_be_bytes());        // maxTime
        buf.extend_from_slice(&0u32.to_be_bytes());          // MEMO_NONE
        buf.extend_from_slice(&7u32.to_be_bytes());          // numOps

        let meta = parse_tx_metadata(&buf).expect("should parse v0 with time bounds");
        assert_eq!(meta.fee, 200);
        assert_eq!(meta.num_ops, 7);
    }

    #[test]
    fn test_parse_tx_v1_with_memo_text() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_be_bytes());          // ENVELOPE_TYPE_TX
        buf.extend_from_slice(&0u32.to_be_bytes());          // KEY_TYPE_ED25519
        buf.extend_from_slice(&[0xAB; 32]);                  // ed25519
        buf.extend_from_slice(&500u32.to_be_bytes());        // fee
        buf.extend_from_slice(&1i64.to_be_bytes());          // seqNum
        buf.extend_from_slice(&0u32.to_be_bytes());          // PRECOND_NONE
        buf.extend_from_slice(&1u32.to_be_bytes());          // MEMO_TEXT
        buf.extend_from_slice(&5u32.to_be_bytes());          // text length = 5
        buf.extend_from_slice(b"hello");                     // text data
        buf.extend_from_slice(&[0, 0, 0]);                   // XDR padding to 8 bytes
        buf.extend_from_slice(&2u32.to_be_bytes());          // numOps

        let meta = parse_tx_metadata(&buf).expect("should parse memo text");
        assert_eq!(meta.fee, 500);
        assert_eq!(meta.num_ops, 2);
    }

    /// Regression: hardcoded fee=0 would make all network TXs equal priority.
    /// With parsing, a TX with fee=10000 should be correctly identified.
    #[test]
    fn test_network_tx_gets_correct_fee_not_zero() {
        let data = build_tx_v1(10000, 1);
        let meta = parse_tx_metadata(&data).expect("should parse");
        assert_ne!(meta.fee, 0, "V-009: network TX must not have fee=0");
        assert_eq!(meta.fee, 10000);
    }
}

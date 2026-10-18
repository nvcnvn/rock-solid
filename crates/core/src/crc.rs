//! Castagnoli CRC-32C checksum calculation and verification (RFC 3720).

use crc::{CRC_32_ISCSI, Crc, Digest, Table};

const CASTAGNOLI: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_ISCSI);

/// Computes the Castagnoli CRC-32C checksum of `data`.
pub const fn crc32c(data: &[u8]) -> u32 {
    CASTAGNOLI.checksum(data)
}

/// Continues a Castagnoli CRC-32C checksum from a previously finalized `prev_crc`.
pub const fn crc32c_append(prev_crc: u32, data: &[u8]) -> u32 {
    let initial = (prev_crc ^ 0xFFFF_FFFF).reverse_bits();
    let mut digest = CASTAGNOLI.digest_with_initial(initial);
    digest.update(data);
    digest.finalize()
}

/// Returns true if the Castagnoli CRC-32C checksum of `data` equals `expected`.
pub const fn verify_crc32c(data: &[u8], expected: u32) -> bool {
    crc32c(data) == expected
}

/// Incremental Castagnoli CRC-32C hasher.
#[derive(Clone)]
pub struct Crc32cHasher {
    digest: Digest<'static, u32, Table<16>>,
}

impl Crc32cHasher {
    /// Creates a new incremental CRC-32C hasher.
    pub const fn new() -> Self {
        Self {
            digest: CASTAGNOLI.digest(),
        }
    }

    /// Feeds `data` into the running checksum.
    pub const fn update(&mut self, data: &[u8]) {
        self.digest.update(data);
    }

    /// Returns the finalized 32-bit checksum.
    pub const fn finalize(self) -> u32 {
        self.digest.finalize()
    }
}

impl Default for Crc32cHasher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3720_appendix_b4_test_vectors() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);

        let zeros = [0x00u8; 32];
        assert_eq!(crc32c(&zeros), 0x8A91_36AA);
        assert_eq!(crc32c(&zeros).to_le_bytes(), [0xAA, 0x36, 0x91, 0x8A]);

        let ones = [0xFFu8; 32];
        assert_eq!(crc32c(&ones), 0x62A8_AB43);
        assert_eq!(crc32c(&ones).to_le_bytes(), [0x43, 0xAB, 0xA8, 0x62]);

        let mut incrementing = [0u8; 32];
        for (i, byte) in incrementing.iter_mut().enumerate() {
            *byte = u8::try_from(i).unwrap();
        }
        assert_eq!(crc32c(&incrementing), 0x46DD_794E);
        assert_eq!(
            crc32c(&incrementing).to_le_bytes(),
            [0x4E, 0x79, 0xDD, 0x46]
        );

        let mut decrementing = [0u8; 32];
        for (i, byte) in decrementing.iter_mut().enumerate() {
            *byte = u8::try_from(31 - i).unwrap();
        }
        assert_eq!(crc32c(&decrementing), 0x113F_DB5C);
        assert_eq!(
            crc32c(&decrementing).to_le_bytes(),
            [0x5C, 0xDB, 0x3F, 0x11]
        );

        let iscsi_read_10_pdu: [u8; 48] = [
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(crc32c(&iscsi_read_10_pdu), 0xD996_3A56);
        assert_eq!(
            crc32c(&iscsi_read_10_pdu).to_le_bytes(),
            [0x56, 0x3A, 0x96, 0xD9]
        );
        assert!(verify_crc32c(&iscsi_read_10_pdu, 0xD996_3A56));
        assert!(!verify_crc32c(&iscsi_read_10_pdu, 0xD996_3A57));
    }

    #[test]
    fn incremental_hasher_and_append_match_oneshot() {
        let payload: Vec<u8> = (0u32..4096)
            .map(|i| (i.wrapping_mul(31) & 0xFF) as u8)
            .collect();
        let expected = crc32c(&payload);

        for split in [0, 1, 7, 16, 63, 64, 511, 2048, 4095, 4096] {
            let (left, right) = payload.split_at(split);

            let mut hasher = Crc32cHasher::new();
            hasher.update(left);
            hasher.update(right);
            assert_eq!(hasher.finalize(), expected);

            let c1 = crc32c(left);
            let c2 = crc32c_append(c1, right);
            assert_eq!(c2, expected);
        }
    }
}

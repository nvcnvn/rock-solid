//! CRC32C of a 4 KiB block: the `crc` crate with one table and with the 16 tables `crates/core`
//! uses, a single-stream SSE4.2 loop (what the parallel-cleaner harness uses), and the `crc32c`
//! crate.

use std::hint::black_box;
use std::time::Instant;

const N: u32 = 2_000_000;

fn naive(data: &[u8]) -> u32 {
    use std::arch::x86_64::_mm_crc32_u64;
    let mut c: u64 = 0xFFFF_FFFF;
    for w in data.as_chunks::<8>().0 {
        // SAFETY: main() checks for SSE4.2.
        c = unsafe { _mm_crc32_u64(c, u64::from_le_bytes(*w)) };
    }
    !(c as u32)
}

fn time(name: &str, buf: &mut [u8], f: impl Fn(&[u8]) -> u32) -> u32 {
    let s = Instant::now();
    let mut x = 0;
    for i in 0..N {
        buf[0] = i as u8;
        x ^= f(black_box(buf));
    }
    println!(
        "CRC lang=rust impl={name} | ns_per_4k={:.0}",
        s.elapsed().as_nanos() as f64 / f64::from(N)
    );
    x
}

fn main() {
    assert!(std::arch::is_x86_feature_detected!("sse4.2"));
    let table = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
    let slice16 = crc::Crc::<u32, crc::Table<16>>::new(&crc::CRC_32_ISCSI);
    let mut buf = vec![0x5au8; 4096];
    let sample = [
        naive(&buf),
        table.checksum(&buf),
        slice16.checksum(&buf),
        crc32c::crc32c(&buf),
    ];
    assert!(
        sample.iter().all(|&c| c == sample[0]),
        "implementations disagree: {sample:?}"
    );
    let mut x = time("crc-3.4-table1", &mut buf, |b| table.checksum(b));
    x ^= time("crc-3.4-table16-as-in-core", &mut buf, |b| {
        slice16.checksum(b)
    });
    x ^= time("sse42-one-stream", &mut buf, naive);
    x ^= time("crc32c-0.6", &mut buf, crc32c::crc32c);
    black_box(x);
}

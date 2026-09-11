//! What does one record cost to parse, by value model?
//!
//! The evidence behind `shapeshift-json`'s tape reader, and behind the note in
//! `BENCHMARKS.md` §7 that the allocator — not the parser — was the bottleneck on the
//! shipped musl binary. Run it on your own hardware and your own data:
//!
//! ```sh
//! cargo run --release --example parsebench -- events.jsonl
//! # and against the shipped artifact's allocator:
//! cargo run --release --target x86_64-unknown-linux-musl --example parsebench -- events.jsonl
//! ```
//!
//! Each arm touches two fields after parsing so the work cannot be optimized away, and
//! all three must agree on the result before any timing is printed.
use std::io::{BufRead, BufReader};
use std::time::Instant;

const SLACK: usize = 64;

fn lines(path: &str) -> Vec<Vec<u8>> {
    let f = BufReader::new(std::fs::File::open(path).unwrap());
    f.lines().map(|l| l.unwrap().into_bytes()).collect()
}

fn main() {
    let path = std::env::args().nth(1).expect("usage: parsebench <file>");
    let src = lines(&path);
    println!("{} records", src.len());
    let mut scratch: Vec<u8> = Vec::with_capacity(4096);

    // Untimed warmup. The three arms run once each, in a fixed order, so without this the
    // first one pays the cold-cache and first-touch cost for `src` and `scratch` and the
    // other two run on warm memory — which would flatter the tape by exactly the amount
    // that matters. These ratios are quoted in BENCHMARKS.md; they should not be an
    // artifact of ordering.
    for l in &src {
        scratch.clear();
        scratch.extend_from_slice(l);
        scratch.reserve(SLACK);
        let _: serde_json::Value = simd_json::serde::from_slice(&mut scratch).unwrap();
    }

    // 1. What ships today: simd-json's serde bridge -> owned serde_json::Value.
    let t = Instant::now();
    let mut sink = 0u64;
    for l in &src {
        scratch.clear();
        scratch.extend_from_slice(l);
        scratch.reserve(SLACK);
        let v: serde_json::Value = simd_json::serde::from_slice(&mut scratch).unwrap();
        if let Some(x) = v.get("id").and_then(|x| x.as_i64()) {
            sink += x as u64;
        }
        if let Some(s) = v
            .get("user")
            .and_then(|u| u.get("name"))
            .and_then(|x| x.as_str())
        {
            sink += s.len() as u64;
        }
    }
    let owned = t.elapsed();

    // 2. Borrowed value: strings borrow the input, but a map is still built per record.
    let t = Instant::now();
    let mut sink2 = 0u64;
    {
        use simd_json::prelude::*;
        for l in &src {
            scratch.clear();
            scratch.extend_from_slice(l);
            scratch.reserve(SLACK);
            let v = simd_json::to_borrowed_value(&mut scratch).unwrap();
            if let Some(x) = v.get("id").and_then(|x| x.as_i64()) {
                sink2 += x as u64;
            }
            if let Some(s) = v
                .get("user")
                .and_then(|u| u.get("name"))
                .and_then(|x| x.as_str())
            {
                sink2 += s.len() as u64;
            }
        }
    }
    let borrowed = t.elapsed();

    // 3. Tape with reused buffers: a flat Vec of nodes, no per-record tree.
    let t = Instant::now();
    let mut sink3 = 0u64;
    let mut buffers = simd_json::Buffers::new(4096);
    use simd_json::prelude::*;
    for l in &src {
        scratch.clear();
        scratch.extend_from_slice(l);
        scratch.reserve(SLACK);
        let tape = simd_json::to_tape_with_buffers(&mut scratch, &mut buffers).unwrap();
        let v = tape.as_value();
        if let Some(x) = v.get("id").and_then(|x| x.as_i64()) {
            sink3 += x as u64;
        }
        // `into_string()` yields `&'input str` — borrowed from the input buffer, not
        // from the cursor, which is what makes a zero-copy string column possible.
        if let Some(s) = v
            .get("user")
            .and_then(|u| u.get("name"))
            .and_then(|x| x.into_string())
        {
            sink3 += s.len() as u64;
        }
    }
    let tape_d = t.elapsed();

    assert_eq!(sink, sink2);
    assert_eq!(sink, sink3);
    let n = src.len() as f64;
    println!(
        "  owned serde_json::Value  {:>7.2?}   {:>9.0} rec/s   1.00x",
        owned,
        n / owned.as_secs_f64()
    );
    println!(
        "  borrowed value           {:>7.2?}   {:>9.0} rec/s   {:.2}x",
        borrowed,
        n / borrowed.as_secs_f64(),
        owned.as_secs_f64() / borrowed.as_secs_f64()
    );
    println!(
        "  tape (reused buffers)    {:>7.2?}   {:>9.0} rec/s   {:.2}x",
        tape_d,
        n / tape_d.as_secs_f64(),
        owned.as_secs_f64() / tape_d.as_secs_f64()
    );
}

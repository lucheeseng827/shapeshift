//! The nesting limit holds end to end: a record at the limit survives every step that
//! recurses once per level, on the smallest stack a run gets; one level more is refused
//! by every reader before any of those steps sees it.

use std::io::Cursor;

use shapeshift_core::{infer_columns, DatasetSpec, Record, Shaper, MAX_RECORD_DEPTH};
use shapeshift_json::{JsonError, JsonlReader, TapeJsonArrayReader, TapeJsonlReader, TapeRecord};

/// `d` objects, one inside the next, around a 1.
fn nested(d: usize) -> String {
    format!("{}1{}", "{\"a\":".repeat(d), "}".repeat(d))
}

/// A spawned thread's default stack, and what serve's workers get: the smallest a run
/// can be on. A debug build, where frames are largest, is what the tests run in.
const SMALLEST_STACK: usize = 2 * 1024 * 1024;

#[test]
fn a_record_at_the_limit_survives_every_recursive_step_on_a_small_stack() {
    let line = nested(MAX_RECORD_DEPTH) + "\n";
    std::thread::Builder::new()
        .stack_size(SMALLEST_STACK)
        .spawn(move || {
            // Owned: built, inferred from (flattened to its one leaf), freed.
            let owned = JsonlReader::new(Cursor::new(line.clone()))
                .next()
                .unwrap()
                .unwrap();
            let cols = infer_columns([&owned], true);
            assert_eq!(cols.len(), 1);
            assert_eq!(cols[0].name.split('.').count(), MAX_RECORD_DEPTH);
            drop(owned);

            // Tape: converted to an owned value (what a `json` column and a drift
            // report do), then shaped into a `json` column with drift on.
            let mut r = TapeJsonlReader::new(Cursor::new(line));
            let tape = r.next_tape().unwrap().unwrap();
            let rec = TapeRecord::new(tape.as_value());
            drop(rec.to_owned_value());
            let spec = DatasetSpec::parse(
                "dataset: d\nschema: strict\noutput: { format: parquet, path: d.parquet }\n\
                 columns:\n  - { name: a, type: json }\ndrift: { policy: warn }\n",
            )
            .unwrap();
            let mut sh = Shaper::from_spec(&spec, &[]).unwrap();
            sh.push(rec).unwrap();
            assert_eq!(sh.flush().unwrap().unwrap().num_rows(), 1);
        })
        .unwrap()
        .join()
        .expect("a record at the nesting limit overflowed a 2 MiB stack");
}

#[test]
fn every_reader_refuses_one_level_more_and_goes_on() {
    let deep = nested(MAX_RECORD_DEPTH + 1);
    let is_depth_reject = |e: JsonError, at: u64| match e {
        JsonError::Parse { line, message, .. } => {
            assert_eq!(line, at);
            assert!(message.contains("nests deeper than"), "{message}");
        }
        other => panic!("expected a depth reject, got {other:?}"),
    };

    let mut r = TapeJsonlReader::new(Cursor::new(format!("{deep}\n{{\"a\":2}}\n")));
    is_depth_reject(r.next_tape().unwrap().err().unwrap(), 1);
    assert!(r.next_tape().unwrap().is_ok());
    assert!(r.next_tape().is_none());

    let mut r = TapeJsonArrayReader::new(Cursor::new(format!("[{deep}, {{\"a\":2}}]"))).unwrap();
    is_depth_reject(r.next_tape().unwrap().err().unwrap(), 1);
    assert!(r.next_tape().unwrap().is_ok());
    assert!(r.next_tape().is_none());

    // Far past the limit, where parsing it used to abort the process.
    let mut r = JsonlReader::new(Cursor::new(nested(100_000) + "\n"));
    is_depth_reject(r.next().unwrap().err().unwrap(), 1);
}

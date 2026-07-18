//! Engine unit tests: inference, coercion, transforms, path selection, spec
//! parse/validate, and the MAR arithmetic.

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use serde_json::json;

use crate::*;

fn col_type(cols: &[ColumnSpec], name: &str) -> ColumnType {
    cols.iter().find(|c| c.name == name).unwrap().ty
}

#[test]
fn infer_detects_scalar_and_temporal_types() {
    let recs = [
        json!({"b": true, "i": 1, "f": 1.5, "s": "hi", "d": "2026-07-13", "t": "2026-07-13T09:00:00Z"}),
        json!({"b": false, "i": 2, "f": 2, "s": "yo", "d": "2026-07-14", "t": "2026-07-14T09:00:00Z"}),
    ];
    let cols = infer_columns(recs.iter(), true);
    assert_eq!(col_type(&cols, "b"), ColumnType::Bool);
    assert_eq!(col_type(&cols, "i"), ColumnType::Int64);
    // an int + a float in the same field widen to float
    assert_eq!(col_type(&cols, "f"), ColumnType::Float64);
    assert_eq!(col_type(&cols, "s"), ColumnType::String);
    assert_eq!(col_type(&cols, "d"), ColumnType::Date);
    assert_eq!(col_type(&cols, "t"), ColumnType::Timestamp);
}

#[test]
fn infer_flattens_nested_and_json_encodes_arrays() {
    let recs = [json!({"user": {"name": "Ada"}, "tags": ["a", "b"]})];
    let cols = infer_columns(recs.iter(), true);
    assert_eq!(col_type(&cols, "user.name"), ColumnType::String);
    assert_eq!(col_type(&cols, "tags"), ColumnType::Json);
    // without flatten, the nested object is a single json column
    let cols2 = infer_columns(recs.iter(), false);
    assert_eq!(col_type(&cols2, "user"), ColumnType::Json);
}

#[test]
fn infer_widens_mixed_scalars_to_string() {
    let recs = [json!({"x": 1}), json!({"x": "text"})];
    let cols = infer_columns(recs.iter(), true);
    assert_eq!(col_type(&cols, "x"), ColumnType::String);
}

#[test]
fn shaper_builds_typed_batch() {
    let cols = vec![
        ColumnSpec {
            name: "id".into(),
            from: None,
            ty: ColumnType::Int64,
            transform: None,
            required: true,
        },
        ColumnSpec {
            name: "amt".into(),
            from: None,
            ty: ColumnType::Float64,
            transform: None,
            required: false,
        },
        ColumnSpec {
            name: "on".into(),
            from: None,
            ty: ColumnType::Bool,
            transform: None,
            required: false,
        },
        ColumnSpec {
            name: "d".into(),
            from: None,
            ty: ColumnType::Date,
            transform: None,
            required: false,
        },
        ColumnSpec {
            name: "t".into(),
            from: None,
            ty: ColumnType::Timestamp,
            transform: None,
            required: false,
        },
        ColumnSpec {
            name: "s".into(),
            from: None,
            ty: ColumnType::String,
            transform: None,
            required: false,
        },
    ];
    let mut sh = Shaper::new(cols, false).unwrap();
    sh.push(&json!({"id": 7, "amt": 1.25, "on": true, "d": "1970-01-02", "t": "1970-01-01T00:00:01Z", "s": "hi"})).unwrap();
    // amt from an integer coerces to float; missing optional → null
    sh.push(&json!({"id": 8, "amt": 3, "on": false})).unwrap();
    let batch = sh.flush().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);

    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.value(0), 7);
    let amt = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(amt.value(0), 1.25);
    assert_eq!(amt.value(1), 3.0);
    let on = batch
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(on.value(0));
    let d = batch
        .column(3)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(d.value(0), 1); // 1970-01-02 == 1 day since epoch
    let t = batch
        .column(4)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(t.value(0), 1_000_000); // 1s in micros
    let s = batch
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(s.value(0), "hi");
    assert!(s.is_null(1)); // missing optional string → null
}

#[test]
fn required_missing_is_rejected_lenient_and_errors_strict() {
    let cols = vec![ColumnSpec {
        name: "id".into(),
        from: None,
        ty: ColumnType::Int64,
        transform: None,
        required: true,
    }];
    // lenient: a required-missing row is a soft reject, not an error
    let mut lenient = Shaper::new(cols.clone(), false).unwrap();
    assert_eq!(
        lenient.push(&json!({"id": 1})).unwrap(),
        PushOutcome::Appended
    );
    assert!(matches!(
        lenient.push(&json!({"nope": 1})).unwrap(),
        PushOutcome::Rejected(_)
    ));
    assert_eq!(lenient.pending(), 1); // only the good row was appended

    // strict: it is a hard error
    let mut strict = Shaper::new(cols, true).unwrap();
    assert!(strict.push(&json!({"nope": 1})).is_err());
}

#[test]
fn bad_optional_value_becomes_null_not_reject() {
    let cols = vec![ColumnSpec {
        name: "n".into(),
        from: None,
        ty: ColumnType::Int64,
        transform: None,
        required: false,
    }];
    let mut sh = Shaper::new(cols, false).unwrap();
    // "abc" is not an int64; optional → null, row still appended
    assert_eq!(
        sh.push(&json!({"n": "abc"})).unwrap(),
        PushOutcome::Appended
    );
    let batch = sh.flush().unwrap().unwrap();
    let n = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(n.is_null(0));
}

#[test]
fn transforms_apply_before_coercion() {
    assert_eq!(Transform::Uppercase.apply(json!("hi")), json!("HI"));
    assert_eq!(Transform::Trim.apply(json!("  x ")), json!("x"));
    assert_eq!(Transform::DollarsToCents.apply(json!(12.5)), json!(1250));
    assert_eq!(
        Transform::JsonEncode.apply(json!({"a": 1})),
        json!("{\"a\":1}")
    );
    assert_eq!(Transform::EmptyToNull.apply(json!("")), json!(null));
    assert_eq!(Transform::Abs.apply(json!(-3)), json!(3));
}

#[test]
fn path_selection_handles_dotted_arrays_and_dollar() {
    let r = json!({"a": {"b": [10, 20]}});
    assert_eq!(select(&r, "a.b.1"), Some(&json!(20)));
    assert_eq!(select(&r, "$.a.b.0"), Some(&json!(10)));
    assert_eq!(select(&r, "a.missing"), None);
    assert_eq!(select(&r, "$"), Some(&r)); // whole record
}

#[test]
fn spec_parses_yaml_and_json_and_validates() {
    let yaml = r#"
dataset: t
output: { format: parquet, path: out.parquet }
columns:
  - { name: id, type: int64, required: true }
"#;
    let s = DatasetSpec::parse(yaml).unwrap();
    assert_eq!(s.dataset, "t");
    assert_eq!(s.output.format, OutputFormat::Parquet);

    let jsonspec = r#"{"dataset":"t","output":{"format":"iceberg","path":"tbl"},"columns":[{"name":"id","type":"long"}]}"#;
    // NB: "long" is not a ColumnType alias; int64 is. Use a valid one:
    let _ = jsonspec;
    let jsonspec = r#"{"dataset":"t","output":{"format":"iceberg","path":"tbl"}}"#;
    let s2 = DatasetSpec::parse(jsonspec).unwrap();
    assert_eq!(s2.output.format, OutputFormat::Iceberg);
}

#[test]
fn spec_validation_rejects_bad_specs() {
    // duplicate column names
    let dup = DatasetSpec::parse(
        r#"{"dataset":"t","output":{"format":"parquet","path":"o"},"columns":[{"name":"a","type":"int64"},{"name":"a","type":"string"}]}"#,
    );
    assert!(dup.is_err());
    // strict mode with no columns
    let strict = DatasetSpec::parse(
        r#"{"dataset":"t","schema":"strict","output":{"format":"parquet","path":"o"}}"#,
    );
    assert!(strict.is_err());
    // partition by an undeclared column is rejected in STRICT mode (the schema is fixed)…
    let part_strict = DatasetSpec::parse(
        r#"{"dataset":"t","schema":"strict","output":{"format":"iceberg","path":"o","partition_by":["x"]},"columns":[{"name":"a","type":"int64"}]}"#,
    );
    assert!(part_strict.is_err());
    // …but in infer mode `columns` is only a partial pin-list, so a partition column need
    // not be declared — the sink resolves it against the inferred schema. Validation passes.
    let part_infer = DatasetSpec::parse(
        r#"{"dataset":"t","output":{"format":"iceberg","path":"o","partition_by":["x"]},"columns":[{"name":"a","type":"int64"}]}"#,
    );
    assert!(part_infer.is_ok());
}

#[test]
fn effective_columns_merges_declared_then_inferred() {
    let spec = DatasetSpec::parse(
        r#"{"dataset":"t","output":{"format":"parquet","path":"o"},"columns":[{"name":"id","type":"int64"}]}"#,
    )
    .unwrap();
    let inferred = infer_columns([json!({"id": 1, "extra": "e"})].iter(), true);
    let eff = effective_columns(&spec, &inferred);
    // declared `id` stays first (and keeps its declared type), `extra` is appended
    assert_eq!(eff[0].name, "id");
    assert!(eff.iter().any(|c| c.name == "extra"));
    // `id` is not duplicated
    assert_eq!(eff.iter().filter(|c| c.name == "id").count(), 1);
}

#[test]
fn infer_date_requires_a_real_calendar_date() {
    assert!(looks_like_date("2026-02-28"));
    assert!(!looks_like_date("2026-02-30")); // shape-valid but not a real date
    assert!(!looks_like_date("2026-13-01"));
    assert!(!looks_like_date("not-a-date"));
}

#[test]
fn abs_transform_stays_total_on_i64_min() {
    // `i64::MIN.abs()` panics; the transform must not.
    let out = Transform::Abs.apply(json!(i64::MIN));
    assert!(out.is_number());
}

#[test]
fn int64_rejects_out_of_range_float_instead_of_saturating() {
    let cols = vec![ColumnSpec {
        name: "n".into(),
        from: None,
        ty: ColumnType::Int64,
        transform: None,
        required: false,
    }];
    let mut sh = Shaper::new(cols, false).unwrap();
    // 1e30 is a finite whole-number float far past i64::MAX — must become null, not
    // saturate to i64::MAX.
    assert_eq!(sh.push(&json!({"n": 1e30})).unwrap(), PushOutcome::Appended);
    let batch = sh.flush().unwrap().unwrap();
    let n = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(n.is_null(0));
}

#[test]
fn mar_estimate_is_honest_both_ways() {
    // 50M rows @ $600/M with a $2 self-host bill → big saving
    let big = estimate_mar(MarInputs {
        rows: 50_000_000,
        vendor_per_million: 600.0,
        self_host_cost: 2.0,
    });
    assert_eq!(big.vendor_cost, 30_000.0);
    assert!(big.saved > 29_000.0);
    // a tiny job that doesn't amortize goes negative
    let tiny = estimate_mar(MarInputs {
        rows: 10,
        vendor_per_million: 600.0,
        self_host_cost: 0.02,
    });
    assert!(tiny.saved < 0.0);
}

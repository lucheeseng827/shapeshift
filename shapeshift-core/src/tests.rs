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

// ---------------------------------------------------------------------------
// Schema drift
// ---------------------------------------------------------------------------

/// A two-column spec over `{"id": int, "amount": float}` — the shape everything below
/// drifts away from. `schema: infer` with an empty inferred list means "these columns,
/// lenient policy": the forgiving default, and the one where a loss is silent.
fn drift_spec(policy: DriftPolicy) -> DatasetSpec {
    DatasetSpec {
        dataset: "t".into(),
        source: Default::default(),
        output: OutputSpec {
            format: OutputFormat::Parquet,
            path: "t.parquet".into(),
            compression: Compression::Snappy,
            partition_by: Vec::new(),
        },
        schema: SchemaMode::Infer,
        columns: vec![
            ColumnSpec {
                name: "id".into(),
                from: None,
                ty: ColumnType::Int64,
                transform: None,
                required: false,
            },
            ColumnSpec {
                name: "amount".into(),
                from: None,
                ty: ColumnType::Float64,
                transform: None,
                required: false,
            },
        ],
        drift: DriftSpec {
            policy,
            ..Default::default()
        },
        options: Default::default(),
    }
}

#[test]
fn drift_detects_a_field_the_schema_never_reaches() {
    let spec = drift_spec(DriftPolicy::Warn);
    let mut sh = Shaper::from_spec(&spec, &[]).unwrap();
    sh.push(&json!({"id": 1, "amount": 1.5})).unwrap();
    // The producer added `currency` (and a nested `meta.source`) after the spec was
    // written. Both used to vanish without a trace.
    sh.push(&json!({"id": 2, "amount": 2.5, "currency": "usd", "meta": {"source": "web"}}))
        .unwrap();
    sh.push(&json!({"id": 3, "amount": 3.5, "currency": "eur"}))
        .unwrap();

    let r = sh.drift_report().unwrap();
    assert!(!r.is_clean());
    assert_eq!(r.rows_scanned, 3);
    assert_eq!(r.rows_with_drift, 2);
    assert_eq!(r.new_fields.len(), 2);
    let cur = &r.new_fields["currency"];
    assert_eq!(cur.count, 2);
    assert_eq!(cur.first_record, 2);
    // Inference's own verdict on the values seen: what to declare to keep them.
    assert_eq!(cur.suggested_type, ColumnType::String);
    // Nested paths are reported the way `flatten` would name the column.
    assert_eq!(r.new_fields["meta.source"].count, 1);
    // Every row is still written — `warn` reports, it does not change the output.
    assert_eq!(sh.flush().unwrap().unwrap().num_rows(), 3);
}

#[test]
fn drift_detects_the_silently_nulled_coercion() {
    let spec = drift_spec(DriftPolicy::Warn);
    let mut sh = Shaper::from_spec(&spec, &[]).unwrap();
    // `amount` starts arriving as a non-numeric string: an optional column under the
    // lenient policy writes null and, before drift detection, said nothing at all.
    sh.push(&json!({"id": 1, "amount": 1.5})).unwrap();
    assert_eq!(
        sh.push(&json!({"id": 2, "amount": "n/a"})).unwrap(),
        PushOutcome::Appended
    );
    let r = sh.drift_report().unwrap();
    let m = &r.type_mismatches["amount"];
    assert_eq!(m.count, 1);
    assert_eq!(m.first_record, 2);
    assert_eq!(m.declared_type, ColumnType::Float64);
    assert_eq!(m.examples, vec![json!("n/a")]);
    // The cell really is null — detection reports the loss, it does not paper over it.
    let batch = sh.flush().unwrap().unwrap();
    let amount = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(amount.is_null(1));
}

#[test]
fn drift_ignores_what_a_column_already_reaches() {
    let mut spec = drift_spec(DriftPolicy::Warn);
    // `user` is kept whole as json, and `tags.0` indexes into the array: neither the
    // subtree under `user` nor the rest of `tags` is a *new* field.
    spec.columns.push(ColumnSpec {
        name: "user".into(),
        from: None,
        ty: ColumnType::Json,
        transform: None,
        required: false,
    });
    spec.columns.push(ColumnSpec {
        name: "first_tag".into(),
        from: Some("tags.0".into()),
        ty: ColumnType::String,
        transform: None,
        required: false,
    });
    let mut sh = Shaper::from_spec(&spec, &[]).unwrap();
    sh.push(&json!({
        "id": 1, "amount": 1.5,
        "user": {"name": "Ada", "plan": "pro"},
        "tags": ["a", "b"]
    }))
    .unwrap();
    assert!(sh.drift_report().unwrap().is_clean());
}

#[test]
fn drift_ignores_nulls_and_the_ignore_policy_tracks_nothing() {
    // A null under an undeclared path carries nothing to lose.
    let spec = drift_spec(DriftPolicy::Warn);
    let mut sh = Shaper::from_spec(&spec, &[]).unwrap();
    sh.push(&json!({"id": 1, "amount": 1.5, "extra": null}))
        .unwrap();
    assert!(sh.drift_report().unwrap().is_clean());

    // `ignore` is the opt-out: no tracker at all.
    let spec = drift_spec(DriftPolicy::Ignore);
    let mut sh = Shaper::from_spec(&spec, &[]).unwrap();
    sh.push(&json!({"id": 1, "amount": 1.5, "extra": "gone"}))
        .unwrap();
    assert!(sh.drift_report().is_none());
}

#[test]
fn drift_rescue_keeps_what_the_schema_would_have_dropped() {
    let spec = drift_spec(DriftPolicy::Rescue);
    let mut sh = Shaper::from_spec(&spec, &[]).unwrap();
    // The rescue column is appended after every declared column, so switching it on is
    // an additive schema change.
    assert_eq!(sh.schema().fields().len(), 3);
    assert_eq!(sh.schema().field(2).name(), "_rescued");
    assert!(sh.schema().field(2).is_nullable());

    sh.push(&json!({"id": 1, "amount": 1.5})).unwrap();
    sh.push(&json!({"id": 2, "amount": "n/a", "currency": "usd"}))
        .unwrap();
    let batch = sh.flush().unwrap().unwrap();
    let rescued = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // A clean row rescues nothing.
    assert!(rescued.is_null(0));
    // A drifted row keeps both the undeclared field and the value that would not coerce.
    let got: serde_json::Value = serde_json::from_str(rescued.value(1)).unwrap();
    assert_eq!(got, json!({"currency": "usd", "amount": "n/a"}));
    assert_eq!(sh.drift_report().unwrap().rows_rescued, 1);
}

#[test]
fn drift_quarantine_keeps_the_output_exactly_on_schema() {
    let spec = drift_spec(DriftPolicy::Quarantine);
    let mut sh = Shaper::from_spec(&spec, &[]).unwrap();
    assert_eq!(
        sh.push(&json!({"id": 1, "amount": 1.5})).unwrap(),
        PushOutcome::Appended
    );
    let out = sh.push(&json!({"id": 2, "amount": 2.5, "currency": "usd"}));
    match out.unwrap() {
        // A quarantined row is a normal reject: counted, and sidecarred for replay.
        PushOutcome::Rejected(reason) => assert!(reason.contains("currency"), "got: {reason}"),
        other => panic!("expected a reject, got {other:?}"),
    }
    assert_eq!(sh.flush().unwrap().unwrap().num_rows(), 1);
    assert_eq!(sh.drift_report().unwrap().rows_quarantined, 1);
}

#[test]
fn drift_error_fails_the_run_on_the_first_event() {
    let spec = drift_spec(DriftPolicy::Error);
    let mut sh = Shaper::from_spec(&spec, &[]).unwrap();
    sh.push(&json!({"id": 1, "amount": 1.5})).unwrap();
    let err = sh
        .push(&json!({"id": 2, "amount": 2.5, "currency": "usd"}))
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("schema drift"), "got: {msg}");
    assert!(msg.contains("record 2"), "got: {msg}");
    assert!(msg.contains("currency"), "got: {msg}");
    // The failed record never reached the builders — the batch is still just row 1.
    assert_eq!(sh.flush().unwrap().unwrap().num_rows(), 1);
}

#[test]
fn drift_report_suggests_the_columns_to_add() {
    let spec = drift_spec(DriftPolicy::Warn);
    let mut sh = Shaper::from_spec(&spec, &[]).unwrap();
    sh.push(&json!({"id": 1, "amount": 1.5, "currency": "usd", "qty": 2}))
        .unwrap();
    let cols = sh.drift_report().unwrap().suggested_columns();
    assert_eq!(cols.len(), 2);
    assert_eq!(col_type(&cols, "currency"), ColumnType::String);
    assert_eq!(col_type(&cols, "qty"), ColumnType::Int64);
}

#[test]
fn drift_is_watched_in_strict_schema_mode_too() {
    // `schema: strict` writes only declared columns — so a field the source grew is
    // refused by design. Refused is not the same as *unreported*.
    let mut spec = drift_spec(DriftPolicy::Warn);
    spec.schema = SchemaMode::Strict;
    let mut sh = Shaper::from_spec(&spec, &[]).unwrap();
    sh.push(&json!({"id": 1, "amount": 1.5, "currency": "usd"}))
        .unwrap();
    assert_eq!(sh.drift_report().unwrap().new_fields["currency"].count, 1);
}

#[test]
fn drift_watches_inferred_schemas_too() {
    // Inference only sees the sample; a field that shows up afterwards has no column,
    // and this is the case the whole feature exists for.
    let mut spec = drift_spec(DriftPolicy::Warn);
    spec.columns.clear();
    let sample = [json!({"id": 1, "amount": 1.5})];
    let inferred = infer_columns(sample.iter(), true);
    let mut sh = Shaper::from_spec(&spec, &inferred).unwrap();
    sh.push(&sample[0]).unwrap();
    sh.push(&json!({"id": 2, "amount": 2.5, "currency": "usd"}))
        .unwrap();
    assert_eq!(sh.drift_report().unwrap().new_fields["currency"].count, 1);
}

#[test]
fn drift_rescue_column_may_not_collide() {
    let mut spec = drift_spec(DriftPolicy::Rescue);
    spec.drift.rescue_column = "amount".into();
    assert!(spec.validate().is_err());
    spec.drift.rescue_column = "  ".into();
    assert!(spec.validate().is_err());
}

#[test]
fn drift_spec_round_trips_and_defaults_to_warn() {
    let spec = DatasetSpec::parse(
        "dataset: t\noutput:\n  format: parquet\n  path: t.parquet\ndrift:\n  policy: quarantine\n",
    )
    .unwrap();
    assert_eq!(spec.drift.policy, DriftPolicy::Quarantine);
    assert_eq!(spec.drift.rescue_column, "_rescued");
    // Absent block → the safe default: detected and reported, never silent.
    let spec =
        DatasetSpec::parse("dataset: t\noutput:\n  format: parquet\n  path: t.parquet\n").unwrap();
    assert_eq!(spec.drift.policy, DriftPolicy::Warn);
    // A default block is not re-emitted, so `shapeshift infer` stays terse.
    assert!(!spec.to_yaml().unwrap().contains("drift:"));
}

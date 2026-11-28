//! Datasets whose input is a source the console pulls (`source.http`, `source.mysql` in the
//! spec) rather than a file: opened from the position the dataset's table records, and the
//! position committed with the rows, as `shapeshift shape` does.
//!
//! Each source is in the build only with its feature (`http`, `mysql`, which the CLI's features
//! of the same names turn on); a spec naming one this build lacks fails its run, saying which.

use std::path::Path;

use anyhow::Result;
use serde_json::{json, Value};
use shapeshift_core::{DatasetSpec, Position, Progress, Source, SourceSpec};
use shapeshift_json::{JsonError, RecordSource};

/// Whether a spec's input is pulled.
pub(crate) fn is_pulled(s: &SourceSpec) -> bool {
    s.http.is_some() || s.mysql.is_some() || s.postgres.is_some()
}

/// A source being read, and the position it started from.
pub(crate) struct Pulled {
    pub source: Box<dyn Source>,
    pub start: Option<Position>,
}

/// What the source reads, as the dataset page shows it: its kind and where it is, without a
/// query string or user info.
pub(crate) fn describe(s: &SourceSpec) -> Option<(&'static str, String)> {
    let shown = |url: &str| {
        let no_query = url.split(['?', '#']).next().unwrap_or(url);
        match no_query.split_once("://") {
            Some((scheme, rest)) => {
                let (authority, path) = rest.split_once('/').map_or((rest, ""), |(a, p)| (a, p));
                let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
                format!("{scheme}://{host}/{path}")
            }
            None => no_query.to_string(),
        }
    };
    if let Some(h) = &s.http {
        return Some(("http", shown(&h.url)));
    }
    for (kind, db) in [("mysql", &s.mysql), ("postgres", &s.postgres)] {
        let Some(m) = db else { continue };
        let what = match (&m.table, &m.query) {
            (Some(t), _) => format!(" table {t}"),
            (_, Some(_)) => " query".to_string(),
            _ => String::new(),
        };
        return Some((kind, format!("{}{what}", shown(&m.url))));
    }
    None
}

/// Whether the source reads by a position (incremental).
pub(crate) fn incremental(s: &SourceSpec) -> bool {
    s.http.as_ref().is_some_and(|h| h.incremental.is_some())
        || s.mysql.as_ref().is_some_and(|m| m.incremental())
        || s.postgres.as_ref().is_some_and(|m| m.incremental())
}

#[cfg(feature = "http")]
fn build_http(spec: &DatasetSpec) -> Result<Option<Box<dyn Source>>> {
    let Some(h) = &spec.source.http else {
        return Ok(None);
    };
    let source = shapeshift_source_http::HttpSource::new(
        h,
        spec.source.csv.clone(),
        spec.source.xml.clone(),
        shapeshift_source_http::HttpOptions::default(),
    )?;
    Ok(Some(Box::new(source)))
}

#[cfg(not(feature = "http"))]
fn build_http(spec: &DatasetSpec) -> Result<Option<Box<dyn Source>>> {
    match &spec.source.http {
        Some(_) => Err(anyhow::anyhow!(
            "this console was built without the HTTP source: build shapeshift with \
             `--features serve,http` to pull from an endpoint"
        )),
        None => Ok(None),
    }
}

#[cfg(feature = "mysql")]
fn build_mysql(spec: &DatasetSpec) -> Result<Option<Box<dyn Source>>> {
    let Some(m) = &spec.source.mysql else {
        return Ok(None);
    };
    let source = shapeshift_source_mysql::MysqlSource::new(
        m,
        shapeshift_source_mysql::MysqlOptions::default(),
    )?;
    Ok(Some(Box::new(source)))
}

#[cfg(not(feature = "mysql"))]
fn build_mysql(spec: &DatasetSpec) -> Result<Option<Box<dyn Source>>> {
    match &spec.source.mysql {
        Some(_) => Err(anyhow::anyhow!(
            "this console was built without the MySQL source: build shapeshift with \
             `--features serve,mysql` to pull from a database"
        )),
        None => Ok(None),
    }
}

#[cfg(feature = "postgres")]
fn build_postgres(spec: &DatasetSpec) -> Result<Option<Box<dyn Source>>> {
    let Some(p) = &spec.source.postgres else {
        return Ok(None);
    };
    let source = shapeshift_source_postgres::PostgresSource::new(
        p,
        shapeshift_source_postgres::PostgresOptions::default(),
    )?;
    Ok(Some(Box::new(source)))
}

#[cfg(not(feature = "postgres"))]
fn build_postgres(spec: &DatasetSpec) -> Result<Option<Box<dyn Source>>> {
    match &spec.source.postgres {
        Some(_) => Err(anyhow::anyhow!(
            "this console was built without the Postgres source: build shapeshift with \
             `--features serve,postgres` to pull from a database"
        )),
        None => Ok(None),
    }
}

/// The position the table at `table` records for the spec's source, if any.
pub(crate) fn recorded(spec: &SourceSpec, table: &Path) -> Result<Option<Value>> {
    let Some((kind, _)) = describe(spec) else {
        return Ok(None);
    };
    if !incremental(spec) {
        return Ok(None);
    }
    Ok(shapeshift_iceberg::source_position_local(table, kind)?)
}

/// Open the spec's pulled source from the position the table at `table` records.
pub(crate) fn open(spec: &DatasetSpec, table: &Path) -> Result<Option<(Pulled, RecordSource)>> {
    let source = match build_http(spec)? {
        Some(s) => s,
        None => match build_mysql(spec)? {
            Some(s) => s,
            None => match build_postgres(spec)? {
                Some(s) => s,
                None => return Ok(None),
            },
        },
    };
    let mut source = source;
    let start = shapeshift_core::resume_from(recorded(&spec.source, table)?, source.as_ref())?;
    let records = source.open(start.clone(), shapeshift_core::Bounds::default())?;
    let records: RecordSource = Box::new(
        records.map(|r| r.map_err(|e| JsonError::Io(std::io::Error::other(e.to_string())))),
    );
    Ok(Some((Pulled { source, start }, records)))
}

/// What a pull did, for the run log and the dataset page.
pub(crate) fn report(p: &Pulled, end: &Option<Position>, progress: &Progress) -> Value {
    json!({
        "kind": p.source.kind(),
        "from": p.start.as_ref().map(Position::to_json),
        "position": end.as_ref().map(Position::to_json),
        "new": progress.new,
        "replayed": progress.replayed,
        "already_read": progress.skipped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_source_is_described_without_its_query_or_user() {
        let spec: DatasetSpec = DatasetSpec::parse(
            "dataset: d\nsource:\n  http:\n    url: https://u@api.example.com/v1/orders?key=s\n\
             output: { format: parquet, path: o.parquet }\n",
        )
        .unwrap();
        assert_eq!(
            describe(&spec.source),
            Some(("http", "https://api.example.com/v1/orders".to_string()))
        );
        assert!(!incremental(&spec.source));
        let m: DatasetSpec = DatasetSpec::parse(
            "dataset: d\nsource:\n  mysql:\n    url: mysql://app@db/shop\n    table: orders\n    cursor: updated_at\noutput: { format: iceberg, path: t, merge_on: [id] }\n",
        )
        .unwrap();
        assert_eq!(
            describe(&m.source),
            Some(("mysql", "mysql://db/shop table orders".to_string()))
        );
        assert!(incremental(&m.source));
    }
}

//! Catalog & engines: what a REST catalog holds, read-only.
//!
//! With `--catalog <uri>` (and `--warehouse`, `--insecure-catalog`, as for `shape --catalog`),
//! the console lists the catalog's namespaces and their tables, and shows a table's history
//! from the metadata the catalog loads for it. It never creates, commits or registers
//! anything. The catalog's credentials come from the environment, as for every command that
//! reaches a catalog: `SHAPESHIFT_CATALOG_TOKEN`, or `SHAPESHIFT_CATALOG_CREDENTIAL`
//! (`client_id:client_secret`, exchanged for a token by a POST to the token endpoint). They
//! are not shown, and the page's snippets for other engines leave a placeholder where they go.
//!
//! A listing walks the whole catalog, so it is kept for [`CACHE_FOR`]; Refresh lists again.
//!
//! It needs a build with `object_store` (the CLI turns on this crate's `catalog` feature with
//! it). Without one, the page says how to get it, and a table request answers 501.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};

use crate::handlers::{parse_body, Ctx};
use crate::http::{Request, Response};

/// How long a listing is shown again before the catalog is asked again.
const CACHE_FOR: Duration = Duration::from_secs(30);

/// Where the console's catalog is, from the command line.
#[derive(Debug, Clone)]
pub struct CatalogSettings {
    pub uri: String,
    pub warehouse: Option<String>,
    /// Let credentials go over plain http to another machine (`--insecure-catalog`).
    pub insecure: bool,
}

/// The last listing, and when it was made.
#[derive(Default)]
pub struct CatalogCache(Mutex<Option<(Instant, Value)>>);

/// Whether this build can reach a catalog.
pub const AVAILABLE: bool = cfg!(feature = "catalog");

#[cfg(feature = "catalog")]
fn connection(s: &CatalogSettings) -> Result<shapeshift_objstore::CatalogConnection, String> {
    let mut c = shapeshift_objstore::CatalogConnection::from_env(&s.uri, s.warehouse.clone())
        .map_err(|e| e.to_string())?;
    c.allow_plain_http = s.insecure;
    Ok(c)
}

/// List the catalog now: its namespaces and tables, or the error (`config` when this console's
/// own settings are wrong, `catalog` when the catalog refused or did not answer).
fn listing(s: &CatalogSettings) -> Value {
    let mut out = json!({ "available": AVAILABLE, "configured": true,
                          "uri": s.uri, "warehouse": s.warehouse, "insecure": s.insecure });
    #[cfg(feature = "catalog")]
    match connection(s) {
        Err(e) => {
            out["error"] = json!(e);
            out["error_kind"] = json!("config");
        }
        Ok(c) => match shapeshift_objstore::browse_catalog(&c) {
            Ok(l) => {
                out["truncated"] = json!(l.truncated);
                out["namespaces"] = json!(l
                    .namespaces
                    .iter()
                    .map(|n| json!({
                        "namespace": n.levels.join("."), "levels": n.levels,
                        "tables": n.tables, "error": n.error,
                    }))
                    .collect::<Vec<_>>());
            }
            Err(e) => {
                out["error"] = json!(e.to_string());
                out["error_kind"] = json!("catalog");
            }
        },
    }
    out["listed_ms"] = json!(crate::runs::now_ms());
    out
}

fn cached_or_list(ctx: &Ctx, s: &CatalogSettings, fresh: bool) -> Value {
    let cache = &ctx.catalog_cache().0;
    if !fresh {
        if let Some((at, v)) = cache.lock().expect("catalog").as_ref() {
            if at.elapsed() < CACHE_FOR {
                return v.clone();
            }
        }
    }
    let v = listing(s);
    *cache.lock().expect("catalog") = Some((Instant::now(), v.clone()));
    v
}

/// `GET /api/catalog`: the catalog's namespaces and tables (or why there are none to show),
/// from a listing at most [`CACHE_FOR`] old.
pub fn get(ctx: &Ctx) -> Response {
    match ctx.catalog() {
        None => Response::json(200, &json!({ "available": AVAILABLE, "configured": false })),
        Some(s) => Response::json(200, &cached_or_list(ctx, s, false)),
    }
}

/// `POST /api/catalog/refresh`: list the catalog again now.
pub fn refresh(ctx: &Ctx) -> Response {
    match ctx.catalog() {
        None => Response::json(200, &json!({ "available": AVAILABLE, "configured": false })),
        Some(s) => Response::json(200, &cached_or_list(ctx, s, true)),
    }
}

#[derive(Deserialize)]
#[cfg_attr(not(feature = "catalog"), allow(dead_code))]
struct TableReq {
    levels: Vec<String>,
    name: String,
}

/// `POST /api/catalog/table`: one table, as the Tables view shows a local one. 500 when this
/// console's catalog settings are wrong, 502 when the catalog refused or did not answer.
pub fn table(ctx: &Ctx, req: &Request) -> Response {
    let r: TableReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let Some(s) = ctx.catalog() else {
        return Response::error(404, "no catalog: start the console with --catalog <uri>");
    };
    #[cfg(feature = "catalog")]
    {
        let label = format!("{}.{}", r.levels.join("."), r.name);
        let c = match connection(s) {
            Ok(c) => c,
            Err(e) => return Response::error(500, e),
        };
        match shapeshift_objstore::catalog_table_metadata(&c, &r.levels, &r.name) {
            Ok(Some((at, meta))) => {
                let mut t = crate::handlers::describe_table(&label, &meta, &[]);
                // Its history is the snapshots the current metadata lists, not files read.
                t["versions_read"] = Value::Null;
                t["metadata_location"] = json!(at);
                t["catalog"] = json!({ "uri": s.uri, "warehouse": s.warehouse });
                Response::json(200, &t)
            }
            Ok(None) => Response::error(404, format!("the catalog has no table {label}")),
            Err(e) => Response::error(502, e.to_string()),
        }
    }
    #[cfg(not(feature = "catalog"))]
    {
        let _ = (s, r);
        Response::error(
            501,
            "this build cannot reach a catalog: build with --features object_store",
        )
    }
}

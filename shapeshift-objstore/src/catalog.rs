//! A minimal Iceberg REST catalog client: enough of the protocol to load a table, create an
//! empty one, and commit to it (a snapshot added, or snapshots expired). The commit's
//! requirements and updates, and the judgement of a commit whose response was lost, are
//! built in `shapeshift-iceberg` ([`catalog_commit`](shapeshift_iceberg::catalog_commit),
//! [`expiry_commit`](shapeshift_iceberg::expiry_commit), [`CatalogCommit::judge`]); this
//! module only carries them.
//!
//! It authenticates the way Iceberg's own clients do (0.5 S1): with a bearer token, or with
//! OAuth2 client credentials exchanged at the catalog's token endpoint for a token that is
//! fetched again before it expires. Secrets come from the environment ([`CatalogAuth::from_env`]),
//! never from a spec, and never appear in an error or a log line.
//!
//! It runs on the same `reqwest` client stack `object_store` already links, so a catalog
//! adds no dependency to the object-store build. Connections are not pooled: a run makes a
//! handful of catalog calls, each from its own short-lived runtime, so each call (a commit
//! above all) gets a connection opened for it rather than one left over from a runtime
//! that has since shut down. The price is one connect per call.

use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use reqwest::header::{HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::Value;
use shapeshift_core::{Result, ShapeError};
use shapeshift_iceberg::{main_snapshot, CatalogCommit, CommitEffect, Reloaded, TableIdent};

use crate::{client_err, sink_err};

/// How long one catalog call may take before its outcome is treated as lost.
pub(crate) const CATALOG_TIMEOUT: Duration = Duration::from_secs(30);
/// Pages a listing follows before giving up on a catalog that never stops paging.
const MAX_PAGES: usize = 1000;
/// Namespaces a browse lists at most.
pub(crate) const MAX_NAMESPACES: usize = 1000;
/// What a catalog without nested namespaces may answer a listing with `?parent=`: a bad or
/// unknown request, a method or parameter it does not support.
const UNSUPPORTED: &[u16] = &[400, 404, 405, 406, 422, 501];

/// How many times an append the catalog refused is built again onto a newer head before
/// it gives up with a conflict (exit 75). Of writers racing on one table each round commits
/// one, so this many lets 17 writers that start together all commit. A merge, a compaction
/// and an expiry are never rebuilt: each was planned against the head it loaded.
pub(crate) const APPEND_REBASES: u32 = 16;

/// How many times a commit whose answer was lost, onto a table that neither moved nor took
/// it, is sent again unchanged before its outcome is reported as unknown (exit 1).
pub(crate) const COMMIT_RESENDS: u32 = 3;

/// How a sink talks to its catalog.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CatalogOptions {
    pub timeout: Duration,
    pub max_rebases: u32,
    pub max_resends: u32,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        CatalogOptions {
            timeout: CATALOG_TIMEOUT,
            max_rebases: APPEND_REBASES,
            max_resends: COMMIT_RESENDS,
        }
    }
}

/// The variable holding a bearer token for the catalog.
pub const TOKEN_ENV: &str = "SHAPESHIFT_CATALOG_TOKEN";
/// The variable holding OAuth2 client credentials for the catalog: `client_id:client_secret`,
/// or a secret alone (the form Iceberg's clients take as `credential`).
pub const CREDENTIAL_ENV: &str = "SHAPESHIFT_CATALOG_CREDENTIAL";
/// The variable naming the token endpoint, when it is not the catalog's own
/// `{uri}/v1/oauth/tokens` (an identity provider in front of the catalog).
pub const OAUTH2_SERVER_URI_ENV: &str = "SHAPESHIFT_CATALOG_OAUTH2_SERVER_URI";
/// The variable naming the scope a token is asked for (default [`DEFAULT_SCOPE`]).
pub const SCOPE_ENV: &str = "SHAPESHIFT_CATALOG_SCOPE";
/// The scope asked for when none is named: Iceberg's clients' default. Polaris wants
/// `PRINCIPAL_ROLE:ALL`.
pub const DEFAULT_SCOPE: &str = "catalog";

/// How shapeshift proves itself to a REST catalog.
#[derive(Clone, Default, PartialEq, Eq)]
pub enum CatalogAuth {
    /// No credentials: a catalog that asks for none (Iceberg's reference fixture).
    #[default]
    None,
    /// A bearer token, sent as is with every request.
    Token(String),
    /// OAuth2 client credentials, exchanged for a bearer token at the token endpoint, and
    /// exchanged again before that token expires.
    ClientCredentials {
        client_id: Option<String>,
        client_secret: String,
        /// The token endpoint; `None` is the catalog's own `{uri}/v1/oauth/tokens`.
        server_uri: Option<String>,
        scope: String,
    },
}

impl fmt::Debug for CatalogAuth {
    /// Names the kind, never a secret: a `Debug` of a connection can end up in a log.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogAuth::None => f.write_str("None"),
            CatalogAuth::Token(_) => f.write_str("Token(<redacted>)"),
            CatalogAuth::ClientCredentials {
                client_id,
                server_uri,
                scope,
                ..
            } => f
                .debug_struct("ClientCredentials")
                .field("client_id", client_id)
                .field("client_secret", &"<redacted>")
                .field("server_uri", server_uri)
                .field("scope", scope)
                .finish(),
        }
    }
}

impl CatalogAuth {
    /// The credentials the environment names: [`TOKEN_ENV`], or [`CREDENTIAL_ENV`] with
    /// [`OAUTH2_SERVER_URI_ENV`] and [`SCOPE_ENV`]; none at all is [`CatalogAuth::None`].
    pub fn from_env() -> Result<Self> {
        for name in [TOKEN_ENV, CREDENTIAL_ENV, OAUTH2_SERVER_URI_ENV, SCOPE_ENV] {
            if let Some(v) = std::env::var_os(name) {
                if v.to_str().is_none() {
                    return Err(ShapeError::Spec(format!(
                        "{name} is not valid UTF-8; set it to plain text, or unset it"
                    )));
                }
            }
        }
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// [`from_env`](Self::from_env) over any lookup, so it can be tested without touching
    /// the process environment. A variable set to nothing counts as unset.
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let get = |name: &str| {
            var(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let (token, credential) = (get(TOKEN_ENV), get(CREDENTIAL_ENV));
        let (server_uri, scope) = (get(OAUTH2_SERVER_URI_ENV), get(SCOPE_ENV));
        match (token, credential) {
            (Some(_), Some(_)) => Err(ShapeError::Spec(format!(
                "both {TOKEN_ENV} and {CREDENTIAL_ENV} are set; set one: a token is sent as is, \
                 a credential is exchanged for a token"
            ))),
            (Some(token), None) => match (server_uri, scope) {
                (None, None) => Ok(CatalogAuth::Token(token)),
                _ => Err(ShapeError::Spec(format!(
                    "{OAUTH2_SERVER_URI_ENV} and {SCOPE_ENV} apply to a credential exchanged \
                     for a token ({CREDENTIAL_ENV}), not to {TOKEN_ENV}; unset them"
                ))),
            },
            (None, Some(credential)) => {
                let (client_id, client_secret) = match credential.split_once(':') {
                    Some((id, secret)) => (Some(id.to_string()), secret.to_string()),
                    None => (None, credential),
                };
                if client_secret.is_empty() {
                    return Err(ShapeError::Spec(format!(
                        "{CREDENTIAL_ENV} names a client but no secret; set it to \
                         client_id:client_secret"
                    )));
                }
                if let Some(u) = &server_uri {
                    if !(u.starts_with("http://") || u.starts_with("https://")) {
                        return Err(ShapeError::Spec(format!(
                            "{OAUTH2_SERVER_URI_ENV} {u:?} must start with http:// or https://"
                        )));
                    }
                    if has_userinfo(u) {
                        return Err(ShapeError::Spec(format!(
                            "{OAUTH2_SERVER_URI_ENV} carries credentials (`user:password@`); \
                             the client's go in {CREDENTIAL_ENV}"
                        )));
                    }
                }
                Ok(CatalogAuth::ClientCredentials {
                    client_id: client_id.filter(|id| !id.is_empty()),
                    client_secret,
                    server_uri,
                    scope: scope.unwrap_or_else(|| DEFAULT_SCOPE.to_string()),
                })
            }
            (None, None) => match (server_uri, scope) {
                (None, None) => Ok(CatalogAuth::None),
                _ => Err(ShapeError::Spec(format!(
                    "{OAUTH2_SERVER_URI_ENV} or {SCOPE_ENV} is set, but no {CREDENTIAL_ENV} to \
                     exchange for a token"
                ))),
            },
        }
    }
}

/// Where a REST catalog is, which of its warehouses, and how to authenticate to it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatalogConnection {
    /// `http(s)://host:port[/path]`; the client appends `/v1/…`.
    pub uri: String,
    /// Sent as `?warehouse=` with `GET /v1/config`: which of the catalog's warehouses (a
    /// Polaris catalog, a Lakekeeper warehouse) the table lives in.
    pub warehouse: Option<String>,
    pub auth: CatalogAuth,
    /// Send credentials over plain `http://` to a machine other than this one: the
    /// insecure mode (`--insecure-catalog`), for a network trusted on its own terms. Off (the
    /// default), a catalog or token endpoint like that is refused when there are credentials
    /// to send, since anyone on the path could read them (OAuth 2.0 requires TLS at the
    /// token endpoint). Nothing turns it on but the caller: not the environment, not a spec.
    pub allow_plain_http: bool,
}

impl CatalogConnection {
    /// A catalog at `uri` that asks for no credentials and has one warehouse.
    pub fn new(uri: impl Into<String>) -> Self {
        CatalogConnection {
            uri: uri.into(),
            ..Default::default()
        }
    }

    /// The catalog at `uri` and `warehouse`, with the credentials the environment names
    /// ([`CatalogAuth::from_env`]). Plain http to another machine stays refused; only
    /// [`allow_plain_http`](Self::allow_plain_http) set by the caller allows it.
    pub fn from_env(uri: impl Into<String>, warehouse: Option<String>) -> Result<Self> {
        Ok(CatalogConnection {
            uri: uri.into(),
            warehouse,
            auth: CatalogAuth::from_env()?,
            allow_plain_http: false,
        })
    }
}

/// A token the catalog will take, and when to fetch the next one.
struct Bearer {
    header: HeaderValue,
    /// `None` for a token that does not expire (one given as is, or one issued without
    /// `expires_in`).
    refresh_at: Option<Instant>,
}

/// A REST catalog at `uri` (`http(s)://host:port`, the client appends `/v1/…`).
pub(crate) struct RestCatalog {
    uri: String,
    /// `{uri}/v1/` plus the catalog's `prefix`, if its config names one.
    root: String,
    /// For reads (config, load), which may follow a redirect.
    client: reqwest::Client,
    /// For writes (create, commit), which never follow one: a 301, 302 or 303 turns a
    /// POST into a GET, whose 200 would read as a commit the catalog never received.
    writes: reqwest::Client,
    /// For the token request: no redirect either, and a proxy only as the token endpoint's
    /// own host allows (see [`is_loopback_uri`]).
    tokens: reqwest::Client,
    auth: CatalogAuth,
    /// The token in use. Shared by every call of the run, each from its own runtime, so
    /// a plain mutex, held only to read or replace it.
    bearer: Mutex<Option<Bearer>>,
}

/// A table as `loadTable` / `createTable` return it.
pub(crate) struct Loaded {
    pub metadata_location: Option<String>,
    pub metadata: Value,
}

/// What became of one commit request.
pub(crate) enum Sent {
    /// The catalog applied it; the new metadata file, when the response says.
    Applied(Option<String>),
    /// The catalog did not apply it, and never will.
    Refused(ShapeError),
    /// No answer that says either way: a 5xx, a timeout, a dropped connection.
    Unknown(String),
}

impl RestCatalog {
    /// Reach the catalog: authenticate, then `GET /v1/config` (with `?warehouse=` when one
    /// is named), whose `prefix` (an override, else a default) goes into every later path.
    pub(crate) async fn connect(conn: &CatalogConnection, timeout: Duration) -> Result<Self> {
        let uri = conn.uri.trim().trim_end_matches('/').to_string();
        if !(uri.starts_with("http://") || uri.starts_with("https://")) {
            return Err(ShapeError::Spec(format!(
                "catalog URI {uri:?} must start with http:// or https://"
            )));
        }
        // The URI is printed in errors, so credentials in it would be too.
        if has_userinfo(&uri) {
            return Err(ShapeError::Spec(format!(
                "the catalog URI carries credentials (`user:password@`); put them in {TOKEN_ENV} \
                 or {CREDENTIAL_ENV} instead"
            )));
        }
        if conn.auth != CatalogAuth::None {
            for target in [Some(uri.as_str()), token_server(&conn.auth)]
                .into_iter()
                .flatten()
            {
                if let Some(host) = plain_http_host(target) {
                    if !conn.allow_plain_http {
                        return Err(ShapeError::Spec(format!(
                            "refusing to send catalog credentials over plain http to {host}, \
                             where anyone on the path could read them: use https (only on a \
                             network trusted on its own terms, --insecure-catalog allows it)"
                        )));
                    }
                    tracing::warn!(%host, "sending catalog credentials over plain http, not https");
                }
            }
        }
        // A request to this machine never goes through a proxy: no proxy can reach this
        // machine's loopback (it would reach its own), and on the way the request's token or
        // secret would cross the network in the clear. Elsewhere the environment's proxy
        // settings apply as they would to any client.
        let builder = |target: &str| {
            let b = reqwest::Client::builder()
                .timeout(timeout)
                .connect_timeout(timeout.min(Duration::from_secs(10)))
                .pool_max_idle_per_host(0)
                .user_agent(concat!("shapeshift/", env!("CARGO_PKG_VERSION")));
            if is_loopback_uri(target) {
                b.no_proxy()
            } else {
                b
            }
        };
        let client = builder(&uri).build().map_err(client_err)?;
        let writes = builder(&uri)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(client_err)?;
        let token_endpoint = token_server(&conn.auth).unwrap_or(&uri);
        let tokens = builder(token_endpoint)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(client_err)?;
        let mut catalog = RestCatalog {
            root: format!("{uri}/v1/"),
            uri,
            client,
            writes,
            tokens,
            auth: conn.auth.clone(),
            bearer: Mutex::new(None),
        };
        let config_url = match conn.warehouse.as_deref().map(str::trim) {
            Some(w) if !w.is_empty() => {
                format!("{}/v1/config?warehouse={}", catalog.uri, encode(w))
            }
            _ => format!("{}/v1/config", catalog.uri),
        };
        let request = catalog.authorized(catalog.client.get(config_url)).await?;
        let uri = catalog.uri.clone();
        let resp = request
            .send()
            .await
            .map_err(|e| sink_err(format!("could not reach the catalog at {uri}: {e}")))?;
        let (status, body) = read(resp).await?;
        if !status.is_success() {
            return Err(sink_err(format!(
                "the catalog at {uri} refused GET /v1/config: {}{}",
                describe(status, &body),
                catalog.auth_hint(status)
            )));
        }
        let config: Value = serde_json::from_slice(&body).map_err(|e| {
            sink_err(format!(
                "the catalog at {uri} sent a config that is not JSON: {e}"
            ))
        })?;
        let prefix = config["overrides"]["prefix"]
            .as_str()
            .or_else(|| config["defaults"]["prefix"].as_str())
            .map(|p| p.trim_matches('/'))
            .filter(|p| !p.is_empty());
        if let Some(p) = prefix {
            catalog.root = format!("{uri}/v1/{p}/");
        }
        Ok(catalog)
    }

    /// `request` with the run's credentials, fetching a token first when there is none
    /// yet or the one in hand is about to expire.
    async fn authorized(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder> {
        let header = match &self.auth {
            CatalogAuth::None => return Ok(request),
            CatalogAuth::Token(token) => bearer_header(token)?,
            CatalogAuth::ClientCredentials { .. } => {
                let fresh = {
                    let held = self.bearer.lock().unwrap_or_else(|e| e.into_inner());
                    held.as_ref()
                        .filter(|b| !b.refresh_at.is_some_and(|at| Instant::now() >= at))
                        .map(|b| b.header.clone())
                };
                match fresh {
                    Some(h) => h,
                    None => {
                        let bearer = self.fetch_token().await?;
                        let header = bearer.header.clone();
                        *self.bearer.lock().unwrap_or_else(|e| e.into_inner()) = Some(bearer);
                        header
                    }
                }
            }
        };
        Ok(request.header(AUTHORIZATION, header))
    }

    /// Exchange the client credentials for a token (OAuth2 `client_credentials` grant, the
    /// form Iceberg's REST spec gives its token endpoint).
    async fn fetch_token(&self) -> Result<Bearer> {
        let CatalogAuth::ClientCredentials {
            client_id,
            client_secret,
            server_uri,
            scope,
        } = &self.auth
        else {
            return Err(sink_err(
                "internal error: no client credentials to exchange",
            ));
        };
        let endpoint = server_uri
            .clone()
            .unwrap_or_else(|| format!("{}/v1/oauth/tokens", self.uri));
        let mut form = format!(
            "grant_type=client_credentials&scope={}&client_secret={}",
            encode(scope),
            encode(client_secret)
        );
        if let Some(id) = client_id {
            form.push_str(&format!("&client_id={}", encode(id)));
        }
        // The token's life counts from when the endpoint issued it, which is no earlier
        // than this: measuring from here keeps the refresh ahead of expiry whatever the
        // answer's delay.
        let asked_at = Instant::now();
        // Through a client that follows no redirect: on a 307 or 308 a client re-sends the
        // body, and the body holds the secret.
        let resp = self
            .tokens
            .post(&endpoint)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(form)
            .send()
            .await
            .map_err(|e| {
                sink_err(format!(
                    "could not reach the token endpoint {endpoint}: {e}"
                ))
            })?;
        let (status, body) = read(resp).await?;
        if !status.is_success() {
            return Err(sink_err(format!(
                "the token endpoint {endpoint} refused the client credentials in {CREDENTIAL_ENV}: \
                 {}",
                describe_oauth(status, &body)
            )));
        }
        let answer: Value = serde_json::from_slice(&body).map_err(|e| {
            sink_err(format!(
                "the token endpoint {endpoint} answered with something not JSON: {e}"
            ))
        })?;
        let token = answer["access_token"]
            .as_str()
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                sink_err(format!(
                    "the token endpoint {endpoint} issued no access_token"
                ))
            })?;
        let refresh_at = refresh_at(&answer["expires_in"], asked_at);
        Ok(Bearer {
            header: bearer_header(token)?,
            refresh_at,
        })
    }

    /// What to say after a 401 or 403: which credentials to set, or that the ones set were
    /// refused.
    fn auth_hint(&self, status: reqwest::StatusCode) -> String {
        if !matches!(status.as_u16(), 401 | 403) {
            return String::new();
        }
        match self.auth {
            CatalogAuth::None => format!(
                " (the catalog asks for credentials: set {TOKEN_ENV}, or {CREDENTIAL_ENV} as \
                 client_id:client_secret)"
            ),
            CatalogAuth::Token(_) => format!(" (the catalog refused the token in {TOKEN_ENV})"),
            CatalogAuth::ClientCredentials { .. } => format!(
                " (the catalog refused the token issued for the credentials in {CREDENTIAL_ENV})"
            ),
        }
    }

    fn namespace_url(&self, t: &TableIdent) -> String {
        // Levels of a namespace are joined by the unit separator (0x1F) in one segment.
        format!(
            "{}namespaces/{}",
            self.root,
            encode(&t.namespace.join("\u{1f}"))
        )
    }

    fn table_url(&self, t: &TableIdent) -> String {
        format!("{}/tables/{}", self.namespace_url(t), encode(&t.name))
    }

    /// `GET url` as JSON, with the run's credentials (with client credentials, getting a
    /// token first is a POST to the token endpoint; the catalog itself is only read). An answer
    /// that is not a success is `Ok(Err(status))` with the error the caller reports.
    async fn get_json(
        &self,
        url: &str,
        what: &str,
    ) -> Result<std::result::Result<Value, (u16, ShapeError)>> {
        let request = self.authorized(self.client.get(url)).await?;
        let resp = request
            .send()
            .await
            .map_err(|e| sink_err(format!("{what} at the catalog {}: {e}", self.uri)))?;
        let (status, body) = read(resp).await?;
        if !status.is_success() {
            return Ok(Err((
                status.as_u16(),
                sink_err(format!(
                    "the catalog at {} refused {what}: {}{}",
                    self.uri,
                    describe(status, &body),
                    self.auth_hint(status)
                )),
            )));
        }
        serde_json::from_slice(&body).map(Ok).map_err(|e| {
            sink_err(format!(
                "the catalog at {} answered {what} with something not JSON: {e}",
                self.uri
            ))
        })
    }

    /// Every item under `key` of a listing at `base`, following `next-page-token` (sent back
    /// as `pageToken`) up to [`MAX_PAGES`] pages. `None` when the first page is refused with one
    /// of `tolerate`; any other refusal, and a catalog that hands back the token it was given, is
    /// an error.
    async fn pages(
        &self,
        base: &str,
        key: &str,
        what: &str,
        tolerate: &[u16],
    ) -> Result<Option<Vec<Value>>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let url = match &token {
                None => base.to_string(),
                Some(t) => {
                    let sep = if base.contains('?') { '&' } else { '?' };
                    format!("{base}{sep}pageToken={}", encode(t))
                }
            };
            let page = match self.get_json(&url, what).await? {
                Ok(page) => page,
                Err((status, _)) if token.is_none() && tolerate.contains(&status) => {
                    return Ok(None)
                }
                Err((_, e)) => return Err(e),
            };
            if let Some(items) = page[key].as_array() {
                out.extend(items.iter().cloned());
            }
            match page["next-page-token"].as_str().filter(|t| !t.is_empty()) {
                Some(t) if token.as_deref() == Some(t) => {
                    return Err(sink_err(format!(
                        "the catalog at {} answered {what} with the page token it was given: it \
                         would never end",
                        self.uri
                    )))
                }
                Some(t) => token = Some(t.to_string()),
                None => return Ok(Some(out)),
            }
        }
        Err(sink_err(format!(
            "the catalog at {} answered {what} with more than {MAX_PAGES} pages",
            self.uri
        )))
    }

    /// Every namespace, as its levels: the top level, then each one's children
    /// (`?parent=`), up to [`MAX_NAMESPACES`]; the flag says whether that cap cut the list short. A
    /// catalog without nested namespaces may refuse `parent` as a bad or unknown request
    /// ([`UNSUPPORTED`]), or ignore it and answer the top level again: either way, no children.
    /// Any other refusal (credentials, a server error) is an error, never a quietly shorter list.
    pub(crate) async fn list_namespaces(&self) -> Result<(Vec<Vec<String>>, bool)> {
        let mut out: Vec<Vec<String>> = Vec::new();
        let mut queue: std::collections::VecDeque<Option<Vec<String>>> = [None].into();
        while let Some(parent) = queue.pop_front() {
            let url = match &parent {
                None => format!("{}namespaces", self.root),
                Some(p) => format!(
                    "{}namespaces?parent={}",
                    self.root,
                    encode(&p.join("\u{1f}"))
                ),
            };
            let tolerate: &[u16] = if parent.is_some() { UNSUPPORTED } else { &[] };
            let Some(found) = self
                .pages(&url, "namespaces", "listing namespaces", tolerate)
                .await?
            else {
                continue;
            };
            for ns in found {
                let levels: Vec<String> = ns
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|l| l.as_str().map(str::to_string))
                    .collect();
                let child = match &parent {
                    None => true,
                    Some(p) => levels.len() > p.len() && levels.starts_with(p),
                };
                if levels.is_empty() || !child || out.contains(&levels) {
                    continue;
                }
                out.push(levels.clone());
                if out.len() >= MAX_NAMESPACES {
                    return Ok((out, true));
                }
                queue.push_back(Some(levels));
            }
        }
        Ok((out, false))
    }

    /// The tables in namespace `ns` (their names), following the catalog's pages.
    pub(crate) async fn list_tables(&self, ns: &[String]) -> Result<Vec<String>> {
        let url = format!(
            "{}namespaces/{}/tables",
            self.root,
            encode(&ns.join("\u{1f}"))
        );
        let found = self
            .pages(&url, "identifiers", "listing tables", &[])
            .await?
            .unwrap_or_default();
        Ok(found
            .iter()
            .filter_map(|i| i["name"].as_str().map(str::to_string))
            .collect())
    }

    /// Load `t`, or `None` when the catalog has no such table (or no such namespace).
    pub(crate) async fn load(&self, t: &TableIdent) -> Result<Option<Loaded>> {
        let request = self.authorized(self.client.get(self.table_url(t))).await?;
        let resp = request
            .send()
            .await
            .map_err(|e| sink_err(format!("loading {t} from the catalog at {}: {e}", self.uri)))?;
        let (status, body) = read(resp).await?;
        match status.as_u16() {
            200 => loaded(&body).map(Some),
            404 => Ok(None),
            _ => Err(sink_err(format!(
                "the catalog at {} refused to load {t}: {}{}",
                self.uri,
                describe(status, &body),
                self.auth_hint(status)
            ))),
        }
    }

    /// Create `t` from `request` (see [`shapeshift_iceberg::create_table_request`]).
    pub(crate) async fn create(&self, t: &TableIdent, request: &Value) -> Result<Created> {
        let request = self
            .writes
            .post(format!("{}/tables", self.namespace_url(t)))
            .header(CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(request).map_err(sink_err)?);
        let sent = self.authorized(request).await?.send().await;
        let resp = match sent {
            Ok(r) => r,
            Err(e) if e.is_connect() => {
                return Err(sink_err(format!(
                    "could not reach the catalog at {} to create {t}: {e}",
                    self.uri
                )))
            }
            Err(e) => return Ok(Created::LoadIt(format!("the create request failed: {e}"))),
        };
        let (status, body) = match read(resp).await {
            Ok(r) => r,
            Err(e) => return Ok(Created::LoadIt(e.to_string())),
        };
        match status.as_u16() {
            // Created, but an answer that cannot be read says nothing of the table: load it.
            200 => Ok(match loaded(&body) {
                Ok(table) => Created::Table(table),
                Err(e) => Created::LoadIt(format!("the create's answer could not be read ({e})")),
            }),
            409 => Ok(Created::LoadIt("another writer created it first".into())),
            // Like a commit's, a create's 5xx can hide one that landed.
            500..=599 => Ok(Created::LoadIt(format!(
                "the catalog answered {}",
                describe(status, &body)
            ))),
            404 => Err(sink_err(format!(
                "the catalog at {} has no namespace `{}` to create {t} in; create the namespace \
                 first ({})",
                self.uri,
                t.namespace.join("."),
                describe(status, &body)
            ))),
            _ => Err(sink_err(format!(
                "the catalog at {} refused to create {t}: {}{}",
                self.uri,
                describe(status, &body),
                self.auth_hint(status)
            ))),
        }
    }

    /// Register `t` as the table whose current metadata is the file at `metadata_location`
    /// (`POST …/namespaces/{ns}/register`, never with `overwrite`). A 409 (the name is
    /// taken, maybe by an earlier landing of this very request), a 5xx or a lost answer is
    /// `LoadIt`: the caller loads `t` and sees what it names.
    pub(crate) async fn register(
        &self,
        t: &TableIdent,
        metadata_location: &str,
    ) -> Result<Created> {
        let body = serde_json::json!({ "name": t.name, "metadata-location": metadata_location });
        let request = self
            .writes
            .post(format!("{}/register", self.namespace_url(t)))
            .header(CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&body).map_err(sink_err)?);
        let sent = self.authorized(request).await?.send().await;
        let resp = match sent {
            Ok(r) => r,
            Err(e) if e.is_connect() => {
                return Err(sink_err(format!(
                    "could not reach the catalog at {} to register {t}: {e}",
                    self.uri
                )))
            }
            Err(e) => return Ok(Created::LoadIt(format!("the register request failed: {e}"))),
        };
        let (status, body) = match read(resp).await {
            Ok(r) => r,
            Err(e) => return Ok(Created::LoadIt(e.to_string())),
        };
        match status.as_u16() {
            200 => Ok(match loaded(&body) {
                Ok(table) => Created::Table(table),
                Err(e) => Created::LoadIt(format!("the register's answer could not be read ({e})")),
            }),
            409 => Ok(Created::LoadIt(
                "the catalog already has a table by that name".into(),
            )),
            500..=599 => Ok(Created::LoadIt(format!(
                "the catalog answered {}",
                describe(status, &body)
            ))),
            404 => Err(sink_err(format!(
                "the catalog at {} has no namespace `{}` to register {t} in; create the \
                 namespace first ({})",
                self.uri,
                t.namespace.join("."),
                describe(status, &body)
            ))),
            _ => Err(sink_err(format!(
                "the catalog at {} refused to register {t}: {}{}",
                self.uri,
                describe(status, &body),
                self.auth_hint(status)
            ))),
        }
    }

    /// Send one commit to `t`. Never errors: every outcome, including "nobody knows", is
    /// a [`Sent`].
    pub(crate) async fn commit(&self, t: &TableIdent, change: &CatalogCommit) -> Sent {
        let bytes = match serde_json::to_vec(&change.body) {
            Ok(b) => b,
            Err(e) => return Sent::Refused(sink_err(e)),
        };
        let request = self
            .writes
            .post(self.table_url(t))
            .header(CONTENT_TYPE, "application/json")
            .body(bytes);
        // No token, no request: nothing left, so nothing can have applied it.
        let request = match self.authorized(request).await {
            Ok(r) => r,
            Err(e) => return Sent::Refused(e),
        };
        let sent = request.send().await;
        let resp = match sent {
            Ok(r) => r,
            // The connection never opened, so the request never left: nothing applied it.
            Err(e) if e.is_connect() => {
                return Sent::Refused(sink_err(format!(
                    "could not reach the catalog at {} to commit to {t}: {e}",
                    self.uri
                )))
            }
            Err(e) => return Sent::Unknown(format!("the commit request failed: {e}")),
        };
        let status = resp.status();
        let resp_location = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = resp.bytes().await.map(|b| b.to_vec());
        match (status.as_u16(), body) {
            (200..=299, Ok(b)) => Sent::Applied(loaded(&b).ok().and_then(|l| l.metadata_location)),
            (200..=299, Err(_)) => Sent::Applied(None),
            (409, b) => Sent::Refused(ShapeError::Conflict(format!(
                "another writer committed to {t} while this one ran, and the catalog refused \
                 this commit ({}); {}. Run it again",
                describe(status, b.as_deref().unwrap_or_default()),
                nothing_done(change),
            ))),
            (500..=599, b) => Sent::Unknown(format!(
                "the catalog answered {}",
                describe(status, b.as_deref().unwrap_or_default())
            )),
            // A redirect is not followed (see `writes`): the request was not taken here.
            (300..=399, _) => Sent::Refused(sink_err(format!(
                "the catalog at {} answered the commit to {t} with a redirect ({status}{}); a \
                 commit is not sent on through a redirect: set the catalog URI to the address \
                 it names",
                self.uri,
                redirect_target(&resp_location),
            ))),
            (_, b) => Sent::Refused(sink_err(format!(
                "the catalog at {} refused the commit to {t}: {}{}",
                self.uri,
                describe(status, b.as_deref().unwrap_or_default()),
                self.auth_hint(status)
            ))),
        }
    }

    /// Commit `change` to `t` and settle its outcome: applied, refused, or, when the answer
    /// was lost, whatever the table loaded again shows.
    pub(crate) async fn commit_and_settle(
        &self,
        t: &TableIdent,
        change: &CatalogCommit,
    ) -> Settled {
        let cause = match self.commit(t, change).await {
            Sent::Applied(location) => return Settled::Committed(location),
            Sent::Refused(error) => return Settled::Refused(error),
            Sent::Unknown(cause) => cause,
        };
        let unknown = |why: String, resend: bool| Settled::Unknown {
            error: sink_err(format!(
                "the commit to {t} may or may not have landed: {cause}, and {why}. {}",
                what_to_check(change)
            )),
            resend,
        };
        match self.load(t).await {
            Ok(Some(table)) => match change.judge(&table.metadata) {
                Reloaded::Committed => Settled::Committed(own_location(&table, change)),
                Reloaded::Refused => Settled::Refused(ShapeError::Conflict(format!(
                    "another writer committed to {t} while this one ran ({cause}; the table \
                     has since moved on without this commit); {}. Run it again",
                    nothing_done(change)
                ))),
                Reloaded::Unknown => unknown(
                    "the table has not moved since, so the commit may still be applied".into(),
                    true,
                ),
            },
            Ok(None) => unknown("the table could not be found afterwards".into(), false),
            Err(e) => unknown(format!("loading the table again failed too ({e})"), false),
        }
    }

    /// After a copy of `change` was refused, when an earlier copy may have landed: settle
    /// what the table holds. Committed if it holds the snapshot; refused if `main` moved
    /// without it (no copy can apply now); unknown otherwise.
    pub(crate) async fn settle_copy(
        &self,
        t: &TableIdent,
        change: &CatalogCommit,
        refusal: ShapeError,
    ) -> Settled {
        let unknown = |why: String| Settled::Unknown {
            error: sink_err(format!(
                "the commit to {t} was sent again after an unanswered attempt, and the copy \
                 was refused ({refusal}); {why}, so whether the first attempt landed cannot be \
                 told. {}",
                what_to_check(change)
            )),
            resend: false,
        };
        match self.load(t).await {
            Ok(Some(table)) => match change.judge(&table.metadata) {
                Reloaded::Committed => Settled::Committed(own_location(&table, change)),
                Reloaded::Refused => Settled::Refused(refusal),
                Reloaded::Unknown => unknown("the table has not moved".into()),
            },
            Ok(None) => unknown("the table could not be found".into()),
            Err(e) => unknown(format!("loading the table failed ({e})")),
        }
    }

    /// [`commit_and_settle`](Self::commit_and_settle), sending the same commit again while
    /// its answer is lost and the table has neither moved nor taken it, up to `max_resends`
    /// times. A refused copy is settled by what the table holds, since an earlier copy may
    /// have landed.
    pub(crate) async fn commit_resending(
        &self,
        t: &TableIdent,
        change: &CatalogCommit,
        max_resends: u32,
    ) -> Settled {
        let mut settled = self.commit_and_settle(t, change).await;
        let mut resends = 0;
        while let Settled::Unknown { resend: true, .. } = settled {
            if resends == max_resends {
                break;
            }
            resends += 1;
            tokio::time::sleep(backoff(resends)).await;
            tracing::info!(table = %t, resends, commit = %change.effect,
                "the catalog's answer was lost and the table has not moved; sending the commit again");
            settled = match self.commit_and_settle(t, change).await {
                Settled::Refused(e) => self.settle_copy(t, change, e).await,
                other => other,
            };
        }
        settled
    }
}

/// A random pause up to 20 ms × 2ⁿ (capped at 320 ms) before the `n`th resend or rebuild, so
/// writers that met on the same commit spread out.
pub(crate) fn backoff(n: u32) -> Duration {
    let ceiling_ms = 20u64 << n.min(4);
    let random = uuid::Uuid::new_v4().as_u128() as u64;
    Duration::from_millis(random % (ceiling_ms + 1))
}

/// What a refused commit leaves behind: nothing it would have published, and (for one that
/// added a snapshot) none of its files.
fn nothing_done(change: &CatalogCommit) -> &'static str {
    match change.effect {
        CommitEffect::AddsSnapshot(_) => {
            "nothing of this write was published, and its files were removed"
        }
        CommitEffect::RemovesSnapshots(_) => "no snapshot was expired, and no file deleted",
    }
}

/// What to do about a commit whose outcome stays unknown.
fn what_to_check(change: &CatalogCommit) -> String {
    match &change.effect {
        CommitEffect::AddsSnapshot(id) => format!(
            "Its files are left in place; check the table's history for snapshot {id} before \
             running this again"
        ),
        // Expiring again is safe whichever way it went: it plans against the table as it
        // then is. Only the files the lost commit would have deleted may stay behind.
        CommitEffect::RemovesSnapshots(_) => "No file was deleted. Running the expiry again is \
             safe; if this one landed, the files only its snapshots read are left in place, \
             unreferenced"
            .into(),
    }
}

/// The metadata file a commit settled by loading the table again wrote: the table's current
/// one while `main` is still this commit's snapshot; once another writer has committed after
/// it, the current file is theirs, and this commit's is not known. An expiry moves no ref,
/// so whose the current file is cannot be told: none is reported.
fn own_location(table: &Loaded, change: &CatalogCommit) -> Option<String> {
    let ours = change.snapshot_id()?;
    (main_snapshot(&table.metadata) == Some(ours))
        .then(|| table.metadata_location.clone())
        .flatten()
}

/// ` to <location>`, when a redirect names one.
fn redirect_target(location: &Option<String>) -> String {
    location
        .as_deref()
        .map(|l| format!(" to {l}"))
        .unwrap_or_default()
}

/// What a create came to.
pub(crate) enum Created {
    Table(Loaded),
    /// The table may well exist (another writer created it, or the answer was lost): load
    /// it, and use it if it is there.
    LoadIt(String),
}

/// What a commit came to, once settled.
pub(crate) enum Settled {
    /// Applied; the new metadata file, when the answer says.
    Committed(Option<String>),
    /// Never applied, and never will be: its files may go. A `Conflict` is another
    /// writer's doing, and an append can be rebuilt past it.
    Refused(ShapeError),
    /// Nobody can tell. With `resend`, the table has neither moved nor taken it, so the same
    /// commit may be sent again: every copy asserts the same head and adds the same
    /// snapshot, so at most one can ever apply, and the table says whether one did.
    Unknown { error: ShapeError, resend: bool },
}

/// Read a response whole.
async fn read(resp: reqwest::Response) -> Result<(reqwest::StatusCode, Vec<u8>)> {
    let status = resp.status();
    let body = resp
        .bytes()
        .await
        .map_err(|e| sink_err(format!("reading the catalog's answer: {e}")))?;
    Ok((status, body.to_vec()))
}

/// A `LoadTableResult`.
fn loaded(body: &[u8]) -> Result<Loaded> {
    let mut v: Value = serde_json::from_slice(body)
        .map_err(|e| sink_err(format!("the catalog's table is not JSON: {e}")))?;
    let metadata = v["metadata"].take();
    if !metadata.is_object() {
        return Err(sink_err("the catalog's answer carries no table metadata"));
    }
    Ok(Loaded {
        metadata_location: v["metadata-location"].as_str().map(str::to_string),
        metadata,
    })
}

/// A status and the catalog's error message (`{"error": {"type", "message"}}`), or the
/// start of the body when it is not one.
fn describe(status: reqwest::StatusCode, body: &[u8]) -> String {
    let error = serde_json::from_slice::<Value>(body)
        .ok()
        .map(|v| v["error"].clone())
        .filter(Value::is_object);
    match error {
        Some(e) => format!(
            "{status} {}: {}",
            e["type"].as_str().unwrap_or("error"),
            e["message"].as_str().unwrap_or("")
        ),
        None => {
            let text = String::from_utf8_lossy(body);
            let text = text.trim();
            match text.char_indices().nth(200) {
                Some((i, _)) => format!("{status}: {}…", &text[..i]),
                None if text.is_empty() => status.to_string(),
                None => format!("{status}: {text}"),
            }
        }
    }
}

/// When to fetch the next token: once nine tenths of this one's life (`expires_in`, in
/// seconds, counted from `asked_at`) have passed, so a call never carries a token that
/// expires on the way. RFC 6749 makes it a number; some providers send a string (`"3599"`)
/// or a fraction, taken alike. `None` (keep the token) when it is absent or unreadable, or
/// too far off to count.
fn refresh_at(expires_in: &Value, asked_at: Instant) -> Option<Instant> {
    let secs = expires_in
        .as_f64()
        .or_else(|| {
            expires_in
                .as_str()
                .and_then(|s| s.trim().parse::<f64>().ok())
        })
        .filter(|s| s.is_finite() && *s >= 0.0)?;
    let life = Duration::try_from_secs_f64(secs * 0.9).ok()?;
    asked_at.checked_add(life)
}

/// Whether an `http(s)://` URI names a user (`user:password@host`) in its authority.
fn has_userinfo(uri: &str) -> bool {
    let rest = uri.split_once("://").map_or(uri, |(_, r)| r);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    authority.contains('@')
}

/// `Bearer <token>`, marked sensitive so it never shows in a `Debug` of a request.
fn bearer_header(token: &str) -> Result<HeaderValue> {
    let mut h = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
        ShapeError::Spec("the catalog token holds characters a header cannot carry".into())
    })?;
    h.set_sensitive(true);
    Ok(h)
}

/// The token endpoint credentials are sent to, when it is not the catalog itself.
fn token_server(auth: &CatalogAuth) -> Option<&str> {
    match auth {
        CatalogAuth::ClientCredentials {
            server_uri: Some(u),
            ..
        } => Some(u),
        _ => None,
    }
}

/// The host of an `http://` URI that is not this machine: where credentials would travel
/// in the clear.
fn plain_http_host(uri: &str) -> Option<&str> {
    let rest = uri.strip_prefix("http://")?;
    let (host, local) = host_of(rest);
    (!local).then_some(host)
}

/// Whether an `http(s)://` URI names this machine (`localhost`, or a loopback address).
fn is_loopback_uri(uri: &str) -> bool {
    let rest = uri.split_once("://").map_or(uri, |(_, r)| r);
    host_of(rest).1
}

/// The `host[:port]` of a URI's authority (`rest` is what follows `scheme://`), without any
/// user part, which is never to be logged; and whether it names this machine.
fn host_of(rest: &str) -> (&str, bool) {
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let name = match host.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(v6),
        None => host.rsplit_once(':').map_or(host, |(h, _)| h),
    };
    let local = name.eq_ignore_ascii_case("localhost")
        || name
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    (host, local)
}

/// The error codes a token endpoint may answer with (RFC 6749 §5.2, and the two it shares
/// with the authorization endpoint).
const OAUTH_ERRORS: [&str; 8] = [
    "invalid_request",
    "invalid_client",
    "invalid_grant",
    "unauthorized_client",
    "unsupported_grant_type",
    "invalid_scope",
    "server_error",
    "temporarily_unavailable",
];

/// A token endpoint's refusal: the status, and which of the OAuth error codes it named, if
/// any. Nothing the endpoint wrote is shown — not its `error_description`, not an error
/// page, not even its `error` unless it is one of [`OAUTH_ERRORS`], printed from that list —
/// since any of it may echo the request it refused, and the request carries the secret.
fn describe_oauth(status: reqwest::StatusCode, body: &[u8]) -> String {
    let v = serde_json::from_slice::<Value>(body).unwrap_or(Value::Null);
    let said = v["error"].as_str().unwrap_or_default();
    match OAUTH_ERRORS.iter().find(|known| **known == said) {
        Some(code) => format!("{status} {code}"),
        None => status.to_string(),
    }
}

/// Percent-encode one path segment (everything but RFC 3986's unreserved characters).
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_segment_keeps_only_unreserved_characters() {
        assert_eq!(encode("analytics\u{1f}raw"), "analytics%1Fraw");
        assert_eq!(encode("a b/c"), "a%20b%2Fc");
        assert_eq!(encode("Events_2026-v1.x~"), "Events_2026-v1.x~");
    }

    #[test]
    fn credentials_come_from_the_environment_and_contradictions_are_refused() {
        let vars = |pairs: &[(&str, &str)]| {
            let pairs: Vec<(String, String)> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            CatalogAuth::from_vars(move |name| {
                pairs
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
            })
        };
        assert_eq!(vars(&[]).unwrap(), CatalogAuth::None);
        assert_eq!(
            vars(&[(TOKEN_ENV, "  ")]).unwrap(),
            CatalogAuth::None,
            "blank is unset"
        );
        assert_eq!(
            vars(&[(TOKEN_ENV, "t0k")]).unwrap(),
            CatalogAuth::Token("t0k".into())
        );
        assert_eq!(
            vars(&[(CREDENTIAL_ENV, "id:se:cret")]).unwrap(),
            CatalogAuth::ClientCredentials {
                client_id: Some("id".into()),
                client_secret: "se:cret".into(),
                server_uri: None,
                scope: DEFAULT_SCOPE.into(),
            },
            "split at the first colon: a secret may hold one"
        );
        assert_eq!(
            vars(&[
                (CREDENTIAL_ENV, "secret-only"),
                (SCOPE_ENV, "PRINCIPAL_ROLE:ALL"),
                (OAUTH2_SERVER_URI_ENV, "https://idp.example/token"),
            ])
            .unwrap(),
            CatalogAuth::ClientCredentials {
                client_id: None,
                client_secret: "secret-only".into(),
                server_uri: Some("https://idp.example/token".into()),
                scope: "PRINCIPAL_ROLE:ALL".into(),
            }
        );
        for (bad, says) in [
            (vec![(TOKEN_ENV, "t"), (CREDENTIAL_ENV, "a:b")], "set one"),
            (
                vec![(TOKEN_ENV, "t"), (SCOPE_ENV, "x")],
                "not to SHAPESHIFT_CATALOG_TOKEN",
            ),
            (vec![(SCOPE_ENV, "x")], "no SHAPESHIFT_CATALOG_CREDENTIAL"),
            (vec![(CREDENTIAL_ENV, "id:")], "no secret"),
            (
                vec![
                    (CREDENTIAL_ENV, "a:b"),
                    (OAUTH2_SERVER_URI_ENV, "idp.example/token"),
                ],
                "must start with http",
            ),
            (
                vec![
                    (CREDENTIAL_ENV, "a:b"),
                    (OAUTH2_SERVER_URI_ENV, "https://u:p@idp.example/token"),
                ],
                "carries credentials",
            ),
        ] {
            let err = vars(&bad).unwrap_err().to_string();
            assert!(err.contains(says), "{bad:?}: {err}");
        }
    }

    #[test]
    fn a_connection_debugs_without_its_secrets() {
        for auth in [
            CatalogAuth::Token("tok-SECRET".into()),
            CatalogAuth::ClientCredentials {
                client_id: Some("me".into()),
                client_secret: "cs-SECRET".into(),
                server_uri: None,
                scope: DEFAULT_SCOPE.into(),
            },
        ] {
            let conn = CatalogConnection {
                uri: "https://c".into(),
                warehouse: Some("w".into()),
                auth,
                allow_plain_http: false,
            };
            let shown = format!("{conn:?}");
            assert!(
                !shown.contains("SECRET") && shown.contains("redacted"),
                "{shown}"
            );
        }
        let h = bearer_header("tok-SECRET").unwrap();
        assert!(!format!("{h:?}").contains("SECRET"));
    }

    #[test]
    fn only_plain_http_to_another_machine_counts_as_in_the_clear() {
        assert_eq!(
            plain_http_host("http://catalog:8181/api"),
            Some("catalog:8181")
        );
        assert_eq!(plain_http_host("http://10.0.0.5"), Some("10.0.0.5"));
        // A name that merely starts like a loopback address is not one.
        assert_eq!(
            plain_http_host("http://127.evil.example/x"),
            Some("127.evil.example")
        );
        // The user part never reaches the log line.
        assert_eq!(
            plain_http_host("http://user:pw@catalog:8181"),
            Some("catalog:8181")
        );
        assert!(has_userinfo("https://user:pw@catalog:8181/api"));
        assert!(is_loopback_uri("https://localhost:8181/api"));
        assert!(is_loopback_uri("http://127.0.0.1:1/v1/oauth/tokens"));
        assert!(is_loopback_uri("http://[::1]:8181"));
        assert!(!is_loopback_uri("https://127.evil.example"));
        assert!(!is_loopback_uri("https://catalog.example:443"));
        assert!(!has_userinfo("https://catalog:8181/api?x=a@b"));
        for local in [
            "https://catalog:8181",
            "http://localhost:8181",
            "http://127.0.0.1:8181/v1",
            "http://[::1]:8181",
        ] {
            assert_eq!(plain_http_host(local), None, "{local}");
        }
    }

    #[test]
    fn a_token_endpoint_refusal_is_described_by_its_oauth_error() {
        let status = reqwest::StatusCode::UNAUTHORIZED;
        let body =
            br#"{"error":"invalid_client","error_description":"Client authentication failed"}"#;
        assert_eq!(
            describe_oauth(status, body),
            "401 Unauthorized invalid_client"
        );
        assert_eq!(
            describe_oauth(status, br#"{"error":"invalid_scope"}"#),
            "401 Unauthorized invalid_scope"
        );
        // Nothing the endpoint wrote beyond a well-formed code is shown: a description, an
        // error page, or a "code" that is really an echo of the request.
        for echo in [
            &br#"{"error":"invalid_client","error_description":"bad secret s:cret"}"#[..],
            b"<html>bad request: client_secret=s%3Acret (s&#58;cret)</html>",
            br#"{"error":"client_secret=s:cret"}"#,
            // An alphanumeric secret echoed as the code itself.
            br#"{"error":"s3cret"}"#,
        ] {
            let shown = describe_oauth(status, echo);
            assert!(!shown.contains("cret"), "{shown}");
            assert!(shown.starts_with("401 Unauthorized"), "{shown}");
        }
        assert_eq!(
            describe_oauth(status, br#"{"error":"s3cret"}"#),
            "401 Unauthorized"
        );
    }

    #[test]
    fn a_token_is_refreshed_at_nine_tenths_of_whatever_life_it_is_given() {
        let in_about = |v: Value| {
            refresh_at(&v, Instant::now())
                .map(|at| at.saturating_duration_since(Instant::now()).as_secs_f64())
        };
        for v in [json!(100), json!(100.0), json!("100"), json!(" 100 ")] {
            let left = in_about(v.clone()).unwrap();
            assert!((89.0..=90.0).contains(&left), "{v}: {left}");
        }
        // Absent, unreadable, negative or past any clock: the token is kept, no panic.
        for v in [
            Value::Null,
            json!("soon"),
            json!(-5),
            json!(u64::MAX),
            json!(1e300),
        ] {
            assert_eq!(in_about(v.clone()), None, "{v}");
        }
    }

    #[test]
    fn an_error_is_described_by_its_type_and_message_not_its_stack() {
        let status = reqwest::StatusCode::CONFLICT;
        let body = br#"{"error":{"message":"Requirement failed","type":"CommitFailedException","code":409,"stack":["a","b"]}}"#;
        assert_eq!(
            describe(status, body),
            "409 Conflict CommitFailedException: Requirement failed"
        );
        assert_eq!(describe(status, b""), "409 Conflict");
        let long = "x".repeat(300);
        assert!(describe(status, long.as_bytes()).ends_with('…'));
    }
}

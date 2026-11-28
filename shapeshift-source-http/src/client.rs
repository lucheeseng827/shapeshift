//! The requests: credentials, retries, redirects, and what may be said about them.

use std::time::{Duration, SystemTime};

use shapeshift_core::{HttpMethod, HttpSourceSpec, Result, ShapeError};
use url::{Host, Origin, Url};

use crate::soap::{self, Soap};
use crate::HttpOptions;

/// The largest response body a page may have.
pub(crate) const MAX_PAGE_BYTES: u64 = 64 * 1024 * 1024;
/// The most redirects one request follows.
const MAX_REDIRECTS: u32 = 5;
/// The longest `Retry-After` a run waits out; a longer one fails the run, saying so.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);
/// The longest wait between retries the backoff grows to.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// A page as it came back.
pub(crate) struct Page {
    /// The URL that answered (after redirects).
    pub url: Url,
    pub body: Vec<u8>,
    /// The `Link: <…>; rel="next"` target, as given.
    pub link_next: Option<String>,
}

/// A credential, and where it goes.
enum Credential {
    Header { name: String, value: String },
    Query { name: String, value: String },
}

pub(crate) struct Client {
    agent: ureq::Agent,
    base: Url,
    origin: Origin,
    method: HttpMethod,
    body: Option<String>,
    headers: Vec<(String, String)>,
    credential: Option<Credential>,
    /// What [`Client::scrub`] takes out of every message: the credential's value, and the
    /// forms it is sent in.
    secrets: Vec<String>,
    /// The SOAP token's password, and its escaped form: taken out whatever its length (a
    /// fault that echoes it would otherwise return it).
    token_secrets: Vec<String>,
    insecure: bool,
    other_origins: bool,
    /// The runner's allow-list of hosts (empty: none given): every request is held to it.
    hosts: Vec<String>,
    /// A SOAP service's envelope and token.
    soap: Option<Soap>,
    retries: u32,
    backoff: Duration,
}

/// `scheme://host[:port]/path`: a URL as this crate prints it, with no query (which may hold
/// a key or a filter) and no user info.
pub fn display_url(u: &Url) -> String {
    let host = u.host_str().unwrap_or("");
    match u.port() {
        Some(p) => format!("{}://{host}:{p}{}", u.scheme(), u.path()),
        None => format!("{}://{host}{}", u.scheme(), u.path()),
    }
}

fn is_loopback(u: &Url) -> bool {
    match u.host() {
        Some(Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

fn env(name: &str, what: &str) -> Result<String> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(ShapeError::Spec(format!(
            "`source.http.auth.{what}` names the environment variable {name}, which is not set \
             (or is empty)"
        ))),
    }
}

fn base64(bytes: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// A `Retry-After` value: seconds, or an HTTP date.
fn retry_after(v: &str) -> Option<Duration> {
    let v = v.trim();
    if let Ok(s) = v.parse::<u64>() {
        return Some(Duration::from_secs(s));
    }
    let at = chrono::DateTime::parse_from_rfc2822(v).ok()?;
    let at = SystemTime::UNIX_EPOCH + Duration::from_secs(u64::try_from(at.timestamp()).ok()?);
    Some(
        at.duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}

/// A wait of about `d`, spread by up to a quarter either way so retrying runs do not meet.
fn jitter(d: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|t| t.subsec_nanos())
        .unwrap_or(0);
    let spread = (f64::from(nanos % 1000) / 1000.0 - 0.5) / 2.0;
    d.mul_f64(1.0 + spread)
}

impl Client {
    pub(crate) fn new(spec: &HttpSourceSpec, options: &HttpOptions) -> Result<Client> {
        let base = Url::parse(&spec.url)
            .map_err(|e| ShapeError::Spec(format!("`source.http.url` is not a URL ({e})")))?;
        if !base.username().is_empty() || base.password().is_some() {
            return Err(ShapeError::Spec(
                "`source.http.url` carries credentials: a spec holds none. Use `auth.basic_env`"
                    .into(),
            ));
        }
        let credential = match &spec.auth {
            None => None,
            Some(a) => Some(if let Some(n) = &a.bearer_env {
                Credential::Header {
                    name: "Authorization".into(),
                    value: format!("Bearer {}", env(n, "bearer_env")?),
                }
            } else if let Some(n) = &a.basic_env {
                let pair = env(n, "basic_env")?;
                if !pair.contains(':') {
                    return Err(ShapeError::Spec(format!(
                        "`source.http.auth.basic_env`: {n} must hold `user:password`"
                    )));
                }
                Credential::Header {
                    name: "Authorization".into(),
                    value: format!("Basic {}", base64(pair.as_bytes())),
                }
            } else if let Some(h) = &a.header_env {
                Credential::Header {
                    name: h.name.clone(),
                    value: env(&h.env, "header_env")?,
                }
            } else if let Some(q) = &a.query_env {
                Credential::Query {
                    name: q.name.clone(),
                    value: env(&q.env, "query_env")?,
                }
            } else {
                unreachable!("validated: one credential")
            }),
        };
        // WS-Security's username token, read now so a missing variable is named up front.
        let token = match spec
            .soap
            .as_ref()
            .and_then(|s| s.username_env.as_ref().zip(s.password_env.as_ref()))
        {
            Some((u, p)) => Some((env(u, "soap.username_env")?, env(p, "soap.password_env")?)),
            None => None,
        };
        let secrets = match &credential {
            None => Vec::new(),
            Some(Credential::Header { value, .. }) => {
                let mut s = vec![value.clone()];
                for scheme in ["Bearer ", "Basic "] {
                    if let Some(raw) = value.strip_prefix(scheme) {
                        s.push(raw.to_string());
                    }
                }
                if let Some(n) = spec.auth.as_ref().and_then(|a| a.basic_env.as_ref()) {
                    if let Ok(pair) = std::env::var(n) {
                        s.extend(pair.split(':').filter(|p| p.len() >= 4).map(String::from));
                        s.push(pair);
                    }
                }
                s
            }
            Some(Credential::Query { value, .. }) => {
                let encoded: String =
                    url::form_urlencoded::byte_serialize(value.as_bytes()).collect();
                vec![value.clone(), encoded]
            }
        };
        let timeout = match &spec.timeout {
            Some(t) => Duration::from_millis(shapeshift_core::duration_ms(t)?.max(1) as u64),
            None => Duration::from_secs(60),
        };
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            .http_status_as_error(false)
            // Redirects are followed here, by origin: ureq drops only `Authorization` on one.
            .max_redirects(0)
            .build()
            .into();
        let mut token_secrets = Vec::new();
        if let Some((_, p)) = &token {
            if !p.is_empty() {
                token_secrets.push(p.clone());
                token_secrets.push(soap::escape(p));
            }
        }
        let client = Client {
            agent,
            origin: base.origin(),
            base,
            method: if spec.soap.is_some() {
                HttpMethod::Post
            } else {
                spec.method.unwrap_or(HttpMethod::Get)
            },
            body: spec.body.clone(),
            headers: spec
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            credential,
            secrets,
            token_secrets,
            insecure: options.insecure,
            other_origins: spec.allow_other_origins,
            hosts: options.hosts.clone(),
            soap: spec.soap.as_ref().map(|s| Soap::new(s, token)),
            retries: spec.retries.unwrap_or(5),
            backoff: options.backoff.unwrap_or(Duration::from_millis(500)),
        };
        client.check(&client.base)?;
        Ok(client)
    }

    pub(crate) fn base(&self) -> &Url {
        &self.base
    }

    /// Take the credential out of a message, whatever put it there.
    pub(crate) fn scrub(&self, e: ShapeError) -> ShapeError {
        let clean = |m: String| {
            // The token's password first, whatever its length; the longest first, so its
            // escaped form is not left half-replaced.
            let mut token: Vec<&String> = self.token_secrets.iter().collect();
            token.sort_by_key(|s| std::cmp::Reverse(s.len()));
            let m = token
                .into_iter()
                .fold(m, |m, s| m.replace(s.as_str(), "***"));
            self.secrets
                .iter()
                // A value this short would take ordinary words with it.
                .filter(|s| s.len() >= 4)
                .fold(m, |m, s| m.replace(s.as_str(), "***"))
        };
        match e {
            ShapeError::Source(m) => ShapeError::Source(clean(m)),
            ShapeError::Spec(m) => ShapeError::Spec(clean(m)),
            other => ShapeError::Source(clean(other.to_string())),
        }
    }

    /// A link the endpoint gave (a `next` path, a `Link` header), against the page's URL.
    pub(crate) fn join(&self, page: &Url, link: &str) -> Result<Url> {
        let u = page.join(link).map_err(|e| {
            ShapeError::Source(format!(
                "{} gave a next-page link that is not a URL ({e})",
                display_url(page)
            ))
        })?;
        if !matches!(u.scheme(), "http" | "https") {
            return Err(ShapeError::Source(format!(
                "{} gave a next-page link that is not http or https",
                display_url(page)
            )));
        }
        Ok(u)
    }

    /// May the run request `u`? Another origin than `url`'s only with `allow_other_origins`
    /// (a page there, or a redirect, may come from the endpoint's data, and could name an
    /// address on this machine's network), and never with the credential or `headers`.
    fn check(&self, u: &Url) -> Result<()> {
        // The runner's list first: it holds whatever the spec allows (another origin too).
        shapeshift_core::check_source_host(
            &self.hosts,
            u.host_str().unwrap_or(""),
            u.port_or_known_default().unwrap_or(0),
        )?;
        if u.origin() != self.origin {
            if self.other_origins {
                return Ok(());
            }
            return Err(ShapeError::Source(format!(
                "{} led to {}, another origin than `source.http.url`'s, so the run stops here: \
                 set `allow_other_origins: true` to follow it (the credential and `headers` are \
                 never sent there)",
                display_url(&self.base),
                display_url(u)
            )));
        }
        if !self.credentialed() {
            return Ok(());
        }
        if u.scheme() == "http" && !is_loopback(u) && !self.insecure {
            return Err(ShapeError::Source(format!(
                "{} is plain http to another machine, and the request carries a credential, \
                 which anyone on the way could read: use https, or pass --insecure-source to \
                 send it anyway",
                display_url(u)
            )));
        }
        Ok(())
    }

    /// One page: retried on 429, a 5xx or a broken connection; redirects followed within
    /// the origin.
    pub(crate) fn get(&self, url: &Url) -> Result<Page> {
        let mut attempt = 0u32;
        loop {
            let wait = match self.once(url) {
                Ok(Answer::Page(p)) => return Ok(p),
                Ok(Answer::Retry { status, after }) => {
                    if attempt >= self.retries {
                        return Err(ShapeError::Source(format!(
                            "{} answered {status} on all {} attempts",
                            display_url(url),
                            attempt + 1
                        )));
                    }
                    if let Some(a) = after.filter(|a| *a > MAX_RETRY_AFTER) {
                        return Err(ShapeError::Source(format!(
                            "{} answered {status} and asks to wait {}s (Retry-After), longer \
                             than a run waits ({}s): run again later",
                            display_url(url),
                            a.as_secs(),
                            MAX_RETRY_AFTER.as_secs()
                        )));
                    }
                    tracing::warn!(url = %display_url(url), status, attempt = attempt + 1,
                        "the endpoint asked to retry");
                    after.unwrap_or_else(|| self.backoff_for(attempt))
                }
                Err(Failure::Transport(m)) => {
                    if attempt >= self.retries {
                        return Err(self.scrub(ShapeError::Source(format!(
                            "{} could not be reached on all {} attempts: {m}",
                            display_url(url),
                            attempt + 1
                        ))));
                    }
                    tracing::warn!(url = %display_url(url), attempt = attempt + 1,
                        "a request failed; retrying");
                    self.backoff_for(attempt)
                }
                Err(Failure::Fatal(e)) => return Err(self.scrub(e)),
            };
            std::thread::sleep(wait);
            attempt += 1;
        }
    }

    fn backoff_for(&self, attempt: u32) -> Duration {
        let d = self.backoff.saturating_mul(1u32 << attempt.min(16));
        jitter(d.min(MAX_BACKOFF))
    }

    /// Whether a request carries a secret: a credential, or a WS-Security token.
    fn credentialed(&self) -> bool {
        self.credential.is_some() || self.soap.as_ref().is_some_and(|s| s.token.is_some())
    }

    /// One request, following redirects.
    fn once(&self, first: &Url) -> std::result::Result<Answer, Failure> {
        let mut url = first.clone();
        let mut method = self.method;
        // A SOAP request's template parameters are the request's, read once: a redirect that
        // keeps the POST names a new URL, not new parameters.
        let soap_params = self.soap.as_ref().map(|s| s.split(first).1);
        for _ in 0..=MAX_REDIRECTS {
            self.check(&url).map_err(Failure::Fatal)?;
            // The credential and the spec's headers go to `url`'s origin only.
            let home = url.origin() == self.origin;
            // A SOAP request's named parameters go in its envelope, the rest in its URL.
            let (mut target, envelope) = match &self.soap {
                Some(s) => {
                    let (t, _) = s.split(&url);
                    let params = soap_params.as_deref().unwrap_or_default();
                    (t, Some(s.envelope(params, home)))
                }
                None => (url.clone(), None),
            };
            if let (true, Some(Credential::Query { name, value })) = (home, &self.credential) {
                target.query_pairs_mut().append_pair(name, value);
            }
            let soap_headers = self.soap.as_ref().map(|s| s.headers());
            let mut headers: Vec<(&str, &str)> = vec![("Accept", self.accept())];
            if let Some((ct, action)) = &soap_headers {
                headers.push(("Content-Type", ct));
                if let Some(a) = action {
                    headers.push(("SOAPAction", a));
                }
            }
            if home {
                headers.extend(self.headers.iter().map(|(k, v)| (k.as_str(), v.as_str())));
                if let Some(Credential::Header { name, value }) = &self.credential {
                    headers.push((name, value));
                }
            }
            let sent = match method {
                HttpMethod::Get => {
                    let mut r = self.agent.get(target.as_str());
                    for (k, v) in &headers {
                        r = r.header(*k, *v);
                    }
                    r.call()
                }
                HttpMethod::Post => {
                    let mut r = self.agent.post(target.as_str());
                    for (k, v) in &headers {
                        r = r.header(*k, *v);
                    }
                    let body = envelope.as_deref().or(self.body.as_deref()).unwrap_or("");
                    r.send(body.as_bytes())
                }
            };
            let mut resp = match sent {
                Ok(r) => r,
                Err(e) => return Err(Failure::Transport(e.to_string())),
            };
            let status = resp.status().as_u16();
            let header = |n: &str| {
                resp.headers()
                    .get(n)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from)
            };
            if matches!(status, 301 | 302 | 303 | 307 | 308) {
                let Some(loc) = header("location") else {
                    return Err(Failure::Fatal(ShapeError::Source(format!(
                        "{} answered {status} with no Location",
                        display_url(&url)
                    ))));
                };
                let next = url.join(&loc).map_err(|e| {
                    Failure::Fatal(ShapeError::Source(format!(
                        "{} redirected to something that is not a URL ({e})",
                        display_url(&url)
                    )))
                })?;
                if url.scheme() == "https"
                    && next.scheme() == "http"
                    && next.origin() == self.origin
                    && self.credentialed()
                {
                    return Err(Failure::Fatal(ShapeError::Source(format!(
                        "{} redirected from https to plain http ({}): the credential is not sent \
                         over it",
                        display_url(&url),
                        display_url(&next)
                    ))));
                }
                if status == 303 {
                    method = HttpMethod::Get;
                }
                url = next;
                continue;
            }
            // Read now: the body is read below, and the response can lend only one of them.
            let retry_after_header = header("retry-after");
            let link_header = header("link");
            // A SOAP service answers a fault with a 500: the fault says what is wrong, and
            // asking again would only hear it again.
            if self.soap.is_some() && status == 500 {
                let body = resp
                    .body_mut()
                    .with_config()
                    .limit(MAX_PAGE_BYTES)
                    .read_to_vec()
                    .unwrap_or_default();
                if let Some(f) = soap::fault(&body) {
                    return Err(Failure::Fatal(ShapeError::Source(format!(
                        "{} answered a SOAP fault: {f}",
                        display_url(&url)
                    ))));
                }
            }
            if status == 429 || (500..=599).contains(&status) {
                return Ok(Answer::Retry {
                    status,
                    after: retry_after_header.as_deref().and_then(retry_after),
                });
            }
            let link_next = link_header.as_deref().and_then(next_link);
            let body = resp
                .body_mut()
                .with_config()
                .limit(MAX_PAGE_BYTES)
                .read_to_vec()
                .map_err(|e| match e {
                    ureq::Error::BodyExceedsLimit(_) => {
                        Failure::Fatal(ShapeError::Source(format!(
                            "a page from {} is larger than {} MiB",
                            display_url(&url),
                            MAX_PAGE_BYTES >> 20
                        )))
                    }
                    other => Failure::Transport(other.to_string()),
                })?;
            if !(200..300).contains(&status) {
                let text = String::from_utf8_lossy(&body[..body.len().min(300)]).into_owned();
                return Err(Failure::Fatal(ShapeError::Source(format!(
                    "{} answered {status}: {}",
                    display_url(&url),
                    text.trim()
                ))));
            }
            return Ok(Answer::Page(Page {
                url,
                body,
                link_next,
            }));
        }
        Err(Failure::Fatal(ShapeError::Source(format!(
            "{} redirected more than {MAX_REDIRECTS} times",
            display_url(first)
        ))))
    }

    fn accept(&self) -> &'static str {
        if self.soap.is_some() {
            return "text/xml, application/soap+xml, application/xml;q=0.9";
        }
        "application/json, application/x-ndjson, text/csv, text/tab-separated-values, application/xml;q=0.9, */*;q=0.1"
    }
}

/// The `rel="next"` target of a `Link` header.
fn next_link(h: &str) -> Option<String> {
    for part in h.split(',') {
        let mut bits = part.split(';');
        let target = bits.next()?.trim();
        let is_next = bits.any(|b| {
            let b = b.trim().to_ascii_lowercase();
            b == "rel=\"next\"" || b == "rel=next"
        });
        if is_next {
            return Some(
                target
                    .trim_start_matches('<')
                    .trim_end_matches('>')
                    .to_string(),
            );
        }
    }
    None
}

enum Answer {
    Page(Page),
    Retry {
        status: u16,
        after: Option<Duration>,
    },
}

enum Failure {
    /// Retried.
    Transport(String),
    /// Not retried.
    Fatal(ShapeError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_the_rfc() {
        for (i, o) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
        ] {
            assert_eq!(base64(i.as_bytes()), o);
        }
    }

    #[test]
    fn link_headers_and_retry_after_read() {
        assert_eq!(
            next_link("<https://a/x?p=1>; rel=\"prev\", <https://a/x?p=3>; rel=\"next\"")
                .as_deref(),
            Some("https://a/x?p=3")
        );
        assert_eq!(next_link("<https://a/x>; rel=\"last\""), None);
        assert_eq!(retry_after("7"), Some(Duration::from_secs(7)));
        assert_eq!(
            retry_after("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(Duration::ZERO)
        );
        assert_eq!(retry_after("soon"), None);
    }

    #[test]
    fn a_displayed_url_drops_its_query() {
        let u = Url::parse("https://api.example.com:8443/v1/orders?api_key=s3cret&x=1").unwrap();
        assert_eq!(display_url(&u), "https://api.example.com:8443/v1/orders");
    }
}

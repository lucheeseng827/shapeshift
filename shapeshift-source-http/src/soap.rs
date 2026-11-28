//! SOAP: the envelope a request POSTs, and a fault read back as an error.

use std::io::{BufReader, Cursor};

use serde_json::Value;
use shapeshift_core::{SoapSpec, SoapVersion, XmlOptions};
use url::Url;

const WSSE_NS: &str =
    "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd";
const PASSWORD_TEXT: &str = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText";

/// A SOAP service's requests.
pub(crate) struct Soap {
    spec: SoapSpec,
    placeholders: Vec<String>,
    /// The WS-Security username token, from the environment.
    pub token: Option<(String, String)>,
}

/// `text` with XML's five special characters escaped.
pub(crate) fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

impl Soap {
    pub fn new(spec: &SoapSpec, token: Option<(String, String)>) -> Soap {
        Soap {
            placeholders: spec.placeholders(),
            spec: spec.clone(),
            token,
        }
    }

    fn v12(&self) -> bool {
        self.spec.version == Some(SoapVersion::V12)
    }

    /// The request's parameters the template names (they go in the body), and the URL with the
    /// rest.
    pub fn split(&self, url: &Url) -> (Url, Vec<(String, String)>) {
        let mut params = Vec::new();
        let mut rest = Vec::new();
        for (k, v) in url.query_pairs() {
            if self.placeholders.iter().any(|p| p == k.as_ref()) {
                params.push((k.into_owned(), v.into_owned()));
            } else {
                rest.push((k.into_owned(), v.into_owned()));
            }
        }
        let mut target = url.clone();
        target.set_query(None);
        if !rest.is_empty() {
            target.query_pairs_mut().extend_pairs(rest);
        }
        (target, params)
    }

    /// The envelope: the template's placeholders filled, the username token in the header when
    /// `secure` (the request goes to the service's own origin).
    pub fn envelope(&self, params: &[(String, String)], secure: bool) -> String {
        // A placeholder with no parameter this request (the lower bound on a first run, which
        // has no position) is empty, as a REST request leaves the parameter out.
        let body = self.spec.render(|name| {
            params
                .iter()
                .find(|(k, _)| k == name)
                .map_or(String::new(), |(_, v)| escape(v))
        });
        let (ns, must) = if self.v12() {
            ("http://www.w3.org/2003/05/soap-envelope", "true")
        } else {
            ("http://schemas.xmlsoap.org/soap/envelope/", "1")
        };
        let header = match (&self.token, secure) {
            (Some((user, password)), true) => format!(
                "<soap:Header><wsse:Security xmlns:wsse=\"{WSSE_NS}\" soap:mustUnderstand=\"{must}\">\
                 <wsse:UsernameToken><wsse:Username>{}</wsse:Username>\
                 <wsse:Password Type=\"{PASSWORD_TEXT}\">{}</wsse:Password>\
                 </wsse:UsernameToken></wsse:Security></soap:Header>",
                escape(user),
                escape(password)
            ),
            _ => String::new(),
        };
        format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><soap:Envelope xmlns:soap=\"{ns}\">\
             {header}<soap:Body>{body}</soap:Body></soap:Envelope>"
        )
    }

    /// The content type, and the `SOAPAction` header (1.1 only: 1.2 carries it in the type).
    pub fn headers(&self) -> (String, Option<String>) {
        let action = self.spec.action.as_deref();
        if self.v12() {
            let ct = match action {
                Some(a) => format!("application/soap+xml; charset=utf-8; action=\"{a}\""),
                None => "application/soap+xml; charset=utf-8".to_string(),
            };
            (ct, None)
        } else {
            (
                "text/xml; charset=utf-8".to_string(),
                Some(format!("\"{}\"", action.unwrap_or(""))),
            )
        }
    }
}

fn text_of(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Object(o) => o.get("#text").and_then(text_of),
        Value::Array(a) => a.first().and_then(text_of),
        _ => None,
    }
}

/// The fault a SOAP body holds, as `code: string` (1.1 `faultcode`/`faultstring`, 1.2
/// `Code/Value`/`Reason/Text`); `None` when the body is no fault.
pub(crate) fn fault(body: &[u8]) -> Option<String> {
    // Most pages are no fault: look for the word before parsing the page.
    if !body.windows(5).any(|w| w == b"Fault") {
        return None;
    }
    let opts = XmlOptions {
        record: "/Envelope/Body/Fault".into(),
    };
    let mut reader = shapeshift_json::XmlReader::new(BufReader::new(Cursor::new(body)), &opts);
    let f = reader.next()?.ok()?;
    let text = f
        .get("faultstring")
        .and_then(text_of)
        .or_else(|| {
            f.get("Reason")
                .and_then(|r| r.get("Text"))
                .and_then(text_of)
        })
        .unwrap_or_else(|| "(no fault string)".to_string());
    let code = f
        .get("faultcode")
        .and_then(text_of)
        .or_else(|| f.get("Code").and_then(|c| c.get("Value")).and_then(text_of));
    Some(match code {
        Some(c) => format!("{c}: {text}"),
        None => text,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn soap(template: &str, version: Option<SoapVersion>) -> Soap {
        Soap::new(
            &SoapSpec {
                action: Some("urn:GetOrders".into()),
                body_template: template.into(),
                version,
                ..SoapSpec::default()
            },
            Some(("ada".into(), "p<w&d".into())),
        )
    }

    #[test]
    fn named_parameters_go_in_the_body_escaped_and_the_rest_stay_in_the_url() {
        let s = soap(
            "<Get><since>{{since}}</since><page>{{ page }}</page></Get>",
            None,
        );
        let u = Url::parse("https://svc/ws?since=a%26b%3Cc&page=2&tenant=7").unwrap();
        let (target, params) = s.split(&u);
        assert_eq!(target.as_str(), "https://svc/ws?tenant=7");
        let env = s.envelope(&params, true);
        assert!(
            env.contains("<since>a&amp;b&lt;c</since><page>2</page>"),
            "{env}"
        );
        assert!(env.contains("<wsse:Username>ada</wsse:Username>"), "{env}");
        assert!(env.contains(">p&lt;w&amp;d</wsse:Password>"), "{env}");
        assert!(env.contains("soap:mustUnderstand=\"1\""), "{env}");
        assert!(
            !s.envelope(&params, false).contains("Security"),
            "no token off the origin"
        );
        assert_eq!(
            s.headers(),
            (
                "text/xml; charset=utf-8".into(),
                Some("\"urn:GetOrders\"".into())
            )
        );
        let v12 = soap("<Get/>", Some(SoapVersion::V12));
        assert_eq!(
            v12.headers().0,
            "application/soap+xml; charset=utf-8; action=\"urn:GetOrders\""
        );
        assert!(v12
            .envelope(&[], true)
            .contains("http://www.w3.org/2003/05/soap-envelope"));
    }

    #[test]
    fn a_placeholder_is_read_once_however_it_is_spaced() {
        let s = soap("<a>{{  since\t}}</a><b>{{page}}</b>", None);
        let env = s.envelope(
            &[
                ("since".into(), "{{page}}".into()),
                ("page".into(), "3".into()),
            ],
            false,
        );
        assert!(env.contains("<a>{{page}}</a><b>3</b>"), "{env}");
    }

    #[test]
    fn faults_read_in_both_versions() {
        let f11 = br#"<?xml version="1.0"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><soap:Fault><faultcode>soap:Client</faultcode><faultstring>Invalid credentials</faultstring></soap:Fault></soap:Body></soap:Envelope>"#;
        assert_eq!(
            fault(f11).as_deref(),
            Some("soap:Client: Invalid credentials")
        );
        let f12 = br#"<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"><env:Body><env:Fault><env:Code><env:Value>env:Sender</env:Value></env:Code><env:Reason><env:Text xml:lang="en">No such order</env:Text></env:Reason></env:Fault></env:Body></env:Envelope>"#;
        assert_eq!(fault(f12).as_deref(), Some("env:Sender: No such order"));
        let ok = br#"<soap:Envelope xmlns:soap="x"><soap:Body><R><Order/></R></soap:Body></soap:Envelope>"#;
        assert_eq!(fault(ok), None);
    }
}

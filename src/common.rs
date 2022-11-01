use reqwest::blocking::RequestBuilder;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

impl FromStr for KeyValue {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sp = s.splitn(2, '=').collect::<Vec<_>>();
        if sp.len() != 2 {
            return Err(anyhow::format_err!(
                "invalid format, expect something like A=B"
            ));
        }
        Ok(KeyValue {
            key: sp[0].to_string(),
            value: sp[1].to_string(),
        })
    }
}

impl From<&KeyValue> for (String, String) {
    fn from(kv: &KeyValue) -> Self {
        (kv.key.clone(), kv.value.clone())
    }
}

pub(crate) fn refine_loki_request(
    req: RequestBuilder,
    headers: Vec<KeyValue>,
    basic_auth: Option<KeyValue>,
    tenant: Option<String>,
) -> RequestBuilder {
    let mut req = req;

    for kv in headers {
        req = req.header(kv.key, kv.value);
    }
    if let Some(auth) = basic_auth {
        req = req.basic_auth(auth.key, Some(auth.value));
    }
    if let Some(t) = tenant {
        req = req.header("X-Scope-OrgID", t);
    }
    req
}

#[allow(dead_code)]
pub(crate) fn red(s: &str) -> String {
    true_color(s, 255, 0, 0)
}

#[allow(dead_code)]
pub(crate) fn green(s: &str) -> String {
    true_color(s, 0, 255, 0)
}

#[allow(dead_code)]
pub(crate) fn blue(s: &str) -> String {
    true_color(s, 0, 0, 255)
}

#[allow(dead_code)]
pub(crate) fn gray(s: &str) -> String {
    true_color(s, 128, 128, 128)
}

fn true_color(s: &str, r: u8, g: u8, b: u8) -> String {
    if atty::is(atty::Stream::Stdout) {
        // should have detect 256 color supports properly
        return format!("\x1b[38;2;{};{};{};1m{}\x1b[0m", r, g, b, s);
    }
    return s.to_string();
}

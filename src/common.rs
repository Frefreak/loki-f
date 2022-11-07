use chrono::NaiveDateTime;
use clap::Args;
use reqwest::blocking::RequestBuilder;
use std::{str::FromStr, time::Duration};
use humantime::parse_duration;

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

pub(crate) fn green(s: &str) -> String {
    true_color(s, 0, 255, 0)
}

pub(crate) fn blue(s: &str) -> String {
    true_color(s, 100, 100, 255)
}

pub(crate) fn yellow(s: &str) -> String {
    true_color(s, 255, 255, 0)
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
    s.to_string()
}

#[derive(Debug, Args)]
pub struct HttpOpts {
    /// Headers to send, used for basic authentication, etc
    #[clap(long, num_args = 0..)]
    pub headers: Vec<KeyValue>,

    /// Send basic auth authentication
    #[clap(short, long, env = "LF_BASIC_AUTH")]
    pub basic_auth: Option<KeyValue>,

    /// Tenant id
    #[clap(short, long, env = "LF_TENANT")]
    pub tenant: Option<String>,

    /// Loki endpoint
    #[clap(
        short,
        long,
        default_value = "http://127.0.0.1:3100",
        env = "LF_ENDPOINT"
    )]
    pub endpoint: String,
}

#[derive(Debug, Args)]
pub struct TimeRangeOpts {
    /// The start time for the query. Defaults to one hour ago.
    #[clap(long)]
    pub start: Option<NaiveDateTime>,

    /// The end time for the query. Defaults to now.
    #[clap(long)]
    pub end: Option<NaiveDateTime>,

    /// Shorthand to specify recent duration as start/end.
    /// This has the highest priority since this is the most
    /// common use case.
    #[clap(long, value_parser=parse_duration)]
    pub since: Option<Duration>,

    /// Shorthand to specify duration (working with start or end).
    /// The interval is [start, start + duration] or [end - duration, end]
    /// depending on whether start or end you have been specified.
    #[clap(short, long, value_parser=parse_duration)]
    pub duration: Option<Duration>,
}

use std::{collections::HashMap, str::FromStr, time::{SystemTime, UNIX_EPOCH}};

use base64::encode;
use clap::Parser;
use serde::Serialize;

/// push a single message (for now, meant for debugging only)
#[derive(Parser, Debug)]
pub struct Push {
    /// labels to use, "prog=lf" if not given
    #[clap(short, long, multiple=true)]
    pub labels: Vec<KeyValue>,

    /// headers to send, used for basic authentication, etc
    #[clap(long, multiple=true)]
    pub headers: Vec<KeyValue>,

    /// send basic auth authentication
    #[clap(short, long)]
    pub basic_auth: Option<KeyValue>,

    /// content to push
    #[clap(short, long)]
    pub content: String,

    /// tenant id
    #[clap(short, long)]
    pub tenant: Option<String>,

    /// loki endpoint
    #[clap(short, long, default_value="http://127.0.0.1:3100")]
    pub endpoint: String,
}

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
            return Err(anyhow::format_err!("invalid format, expect something like A=B"));
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

#[derive(Debug, Serialize)]
struct PushRequest {
    streams: Vec<Stream>
}

#[derive(Debug, Serialize)]
struct Stream {
    stream: HashMap<String, String>,
    values: Vec<(String, String)>,
}

pub fn push(p: Push) -> anyhow::Result<()> {
    let req = mk_req(&p);
    let payload = serde_json::to_string(&req)?;
    let client = reqwest::blocking::Client::new();
    let mut req = client.post(format!("{}/loki/api/v1/push", p.endpoint))
        .header("Content-Type", "application/json");
    for kv in p.headers {
        req = req.header(kv.key, kv.value);
    }
    if let Some(auth) = p.basic_auth {
        let s = format!("{}:{}", auth.key, auth.value);
        let encoded = encode(s);
        req = req.header("Authorization", format!("Basic {}", encoded));
    }
    if let Some(t) = p.tenant {
        req = req.header("X-Scope-OrgID", t);
    }
    let resp = req.body(payload).send()?;
    println!("{}\n{}", resp.status(), resp.text()?);
    Ok(())
}

fn mk_req(push: &Push) -> PushRequest {
    let labels = if push.labels.is_empty() {
        vec![KeyValue{ key: "prog".to_string(), value: "lf".to_string() }]
    } else {
        push.labels.clone()
    };
    let stream: HashMap<String, String> = labels.iter().map(|x| x.into()).collect();
    let now = SystemTime::now();
    let ts = now.duration_since(UNIX_EPOCH).expect("get timestamp").as_nanos() as i64;
    let values = vec![(ts.to_string(), push.content.clone())];
    PushRequest {
        streams: vec![Stream{ stream, values }]
    }
}

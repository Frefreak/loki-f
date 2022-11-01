use std::{collections::HashMap, time::{SystemTime, UNIX_EPOCH}};

use clap::Parser;
use serde::Serialize;

use crate::common::{KeyValue, refine_loki_request};

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
    let req = client.post(format!("{}/loki/api/v1/push", p.endpoint))
        .header("Content-Type", "application/json");
    let req = refine_loki_request(req, p.headers, p.basic_auth, p.tenant);
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

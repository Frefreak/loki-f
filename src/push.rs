use std::{collections::HashMap, time::{SystemTime, UNIX_EPOCH}};

use clap::Parser;
use serde::Serialize;

use crate::common::{KeyValue, refine_loki_request};

/// push a single message (for now, meant for debugging only)
#[derive(Parser, Debug)]
pub struct Push {
    /// Labels to use, "prog=lf" if not given
    #[clap(short, long, num_args=0..)]
    labels: Vec<KeyValue>,

    /// Headers to send, used for basic authentication, etc
    #[clap(long, num_args=0..)]
    headers: Vec<KeyValue>,

    /// Send basic auth authentication
    #[clap(short, long, env = "LF_BASIC_AUTH")]
    basic_auth: Option<KeyValue>,

    /// Content to push
    #[clap(short, long)]
    content: String,

    /// Tenant id
    #[clap(short, long, env = "LF_TENANT")]
    tenant: Option<String>,

    /// Loki endpoint
    #[clap(short, long, default_value="http://127.0.0.1:3100", env = "LF_ENDPOINT")]
    endpoint: String,
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

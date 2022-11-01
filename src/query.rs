use parse_duration::parse as parse_duration;
use serde::Serialize;
use std::{
    str::FromStr,
    time::Duration,
};
use tracing::debug;

use chrono::{Local, NaiveDateTime};
use clap::Parser;

use crate::common::{refine_loki_request, KeyValue, green, gray, blue};

#[derive(Parser, Debug)]
/// loki query range api
pub struct Query {
    /// headers to send, used for basic authentication, etc
    #[clap(long, multiple = true)]
    pub headers: Vec<KeyValue>,

    /// send basic auth authentication
    #[clap(short, long)]
    pub basic_auth: Option<KeyValue>,

    /// The LogQL query to perform
    #[clap(short, long)]
    query: String,

    /// The max number of entries to return. Only applies
    /// to query types which produce a stream(log lines) response.
    #[clap(short, long, default_value = "100")]
    limit: u32,

    /// print raw response json
    #[clap(short, long)]
    raw: bool,

    /// The start time for the query. Defaults to one hour ago.
    #[clap(long)]
    start: Option<NaiveDateTime>,

    /// The end time for the query. Defaults to now.
    #[clap(long)]
    end: Option<NaiveDateTime>,

    /// Determines the sort order of logs. Supported values are forward or backward
    #[clap(long, default_value = "backward")]
    direction: QueryDirection,

    /// shorthand to specify recent duration as start/end
    /// This has the highest priority since this is the most
    /// common use case.
    #[clap(long, parse(try_from_str=parse_duration))]
    since: Option<Duration>,

    /// shorthand to specify duration (working with start or end)
    /// the interval is [start, start + duration] or [end - duration, end]
    /// depending on whether start or end you have been specified.
    #[clap(short, long, parse(try_from_str=parse_duration))]
    duration: Option<Duration>,

    /// tenant id
    #[clap(short, long)]
    pub tenant: Option<String>,

    /// loki endpoint
    #[clap(short, long, default_value = "http://127.0.0.1:3100")]
    pub endpoint: String,
}

#[derive(Debug, Serialize)]
enum QueryDirection {
    #[serde(rename = "forward")]
    Forward,
    #[serde(rename = "backward")]
    Backward,
}

impl FromStr for QueryDirection {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "backward" => Ok(QueryDirection::Backward),
            "forward" => Ok(QueryDirection::Forward),
            _ => Err(anyhow::format_err!("invalid choice")),
        }
    }
}

#[derive(Debug, Serialize)]
struct QueryRangeRequest {
    // nanoseconds
    start: i64,
    end: i64,
    limit: u32,
    direction: QueryDirection,
    query: String,
}

pub fn query(q: Query) -> anyhow::Result<()> {
    debug!("{q:?}");
    let (from, through) = get_duration(&q)?;
    let client = reqwest::blocking::Client::new();
    let req = client.get(format!("{}/loki/api/v1/query_range", q.endpoint));
    let req = refine_loki_request(req, q.headers, q.basic_auth, q.tenant);
    let query = QueryRangeRequest {
        start: from.timestamp_nanos(),
        end: through.timestamp_nanos(),
        limit: q.limit,
        direction: q.direction,
        query: q.query,
    };
    debug!("{query:?}");
    let resp = req.query(&query).send()?;
    println!("{}", resp.status());
    let obj: serde_json::Value = serde_json::from_str(&resp.text()?)?;
    if q.raw {
        println!("{}", serde_json::to_string_pretty(&obj)?);
    }
    let result = obj.get("data").unwrap().get("result").unwrap();
    for r in result.as_array().unwrap() {
        // labels
        let stream = r.get("stream").unwrap();
        let mut stream_label = String::default();
        for (k, v) in stream.as_object().unwrap() {
            stream_label.push_str(&format!("{} = {}", k, v.as_str().unwrap()));
        }
        println!("{}", green(&stream_label));

        // values
        for value in r.get("values").unwrap().as_array().unwrap() {
            let ts_nano = value[0].as_str().unwrap().parse::<u64>().unwrap();
            let date = NaiveDateTime::from_timestamp(
                (ts_nano / 1000_000_000) as i64,
                (ts_nano % 1000_000_000) as u32,
            );
            let text = value[1].as_str().unwrap();
            let date_str = date.format("%Y-%m-%d %H:%M:%S").to_string();
            println!("{} {} {text}", gray(&date_str), blue("|"));
        }
    }
    Ok(())
}

fn get_duration(q: &Query) -> anyhow::Result<(NaiveDateTime, NaiveDateTime)> {
    if let Some(since) = q.since {
        if q.start.is_some() || q.end.is_some() || q.duration.is_some() {
            return Err(anyhow::format_err!("'since' prohibit start/end/duration"));
        }
        let since = chrono::Duration::from_std(since)?;
        debug!("{}", Local::now());
        let now = Local::now().naive_utc();
        let start = now
            .checked_sub_signed(since)
            .ok_or(anyhow::format_err!("failed to compute 'from' time"))?;
        return Ok((start, now));
    }
    if let Some(duration) = q.duration {
        if q.start.is_some() && q.end.is_some() {
            return Err(anyhow::format_err!(
                "'duration' expects 'start' or 'end', not both"
            ));
        }
        if q.start.is_none() && q.end.is_none() {
            return Err(anyhow::format_err!(
                "'duration' expects 'start' or 'end', neither given"
            ));
        }
        let duration = chrono::Duration::from_std(duration)?;
        if let Some(start) = q.start {
            let end = start
                .checked_add_signed(duration)
                .ok_or(anyhow::format_err!("failed to compute 'end' time"))?;
            return Ok((start, end));
        }
        if let Some(end) = q.end {
            let start = end
                .checked_sub_signed(duration)
                .ok_or(anyhow::format_err!("failed to compute 'start' time"))?;
            return Ok((start, end));
        }
    }
    if q.start.is_none() || q.end.is_none() {
        return Err(anyhow::format_err!("'start' and 'end' expected"));
    }
    Ok((q.start.unwrap(), q.end.unwrap()))
}

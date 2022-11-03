use serde::Serialize;
use std::{str::FromStr, time::Duration};
use tracing::debug;

use chrono::{Local, NaiveDateTime};
use clap::{Parser, ValueEnum};

use crate::common::{blue, gray, green, refine_loki_request, HttpOpts, TimeRangeOpts};

#[derive(Parser, Debug)]
/// loki query range api
pub struct Query {
    #[command(flatten)]
    http: HttpOpts,

    #[command(flatten)]
    time_range: TimeRangeOpts,

    /// The LogQL query to perform
    #[clap(short, long, default_value="{prog=\"lf\"}")]
    query: String,

    /// The max number of entries to return. Only applies
    /// to query types which produce a stream(log lines) response.
    #[clap(short, long, default_value = "100")]
    limit: u32,

    /// Print raw response json
    #[clap(short, long)]
    raw: bool,

    /// Determines the sort order of logs. Supported values are forward or backward
    #[clap(long, default_value = "backward", value_enum)]
    direction: QueryDirection,
}

#[derive(Debug, Serialize, Clone, ValueEnum)]
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
    let (from, through) = get_duration(&q.time_range)?;
    let client = reqwest::blocking::Client::new();
    let req = client.get(format!("{}/loki/api/v1/query_range", q.http.endpoint));
    let req = refine_loki_request(req, q.http.headers, q.http.basic_auth, q.http.tenant);
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
        let mut first = true;
        for (k, v) in stream.as_object().unwrap() {
            if first {
                stream_label.push_str(&format!("{} = {}", k, v.as_str().unwrap()));
                first = false;
            } else {
                stream_label.push_str(&format!(", {} = {}", k, v.as_str().unwrap()));
            }
        }
        println!("{}", green(&stream_label));

        // values
        for value in r.get("values").unwrap().as_array().unwrap() {
            let ts_nano = value[0].as_str().unwrap().parse::<u64>().unwrap();
            let date = NaiveDateTime::from_timestamp(
                (ts_nano / 1_000_000_000) as i64,
                (ts_nano % 1_000_000_000) as u32,
            );
            let text = value[1].as_str().unwrap();
            let date_str = date.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
            println!("{} {} {text}", gray(&date_str), blue("|"));
        }
    }
    Ok(())
}

fn get_duration_helper(
    start: Option<NaiveDateTime>,
    end: Option<NaiveDateTime>,
    duration: Option<Duration>,
    since: Option<Duration>,
) -> anyhow::Result<(NaiveDateTime, NaiveDateTime)> {
    if let Some(since) = since {
        if start.is_some() || end.is_some() || duration.is_some() {
            return Err(anyhow::format_err!("'since' prohibit start/end/duration"));
        }
        let since = chrono::Duration::from_std(since)?;
        debug!("{}", Local::now());
        let now = Local::now().naive_utc();
        let start = now
            .checked_sub_signed(since)
            .ok_or_else(|| anyhow::format_err!("failed to compute 'from' time"))?;
        return Ok((start, now));
    }
    if let Some(duration) = duration {
        if start.is_some() && end.is_some() {
            return Err(anyhow::format_err!(
                "'duration' expects 'start' or 'end', not both"
            ));
        }
        if start.is_none() && end.is_none() {
            return Err(anyhow::format_err!(
                "'duration' expects 'start' or 'end', neither given"
            ));
        }
        let duration = chrono::Duration::from_std(duration)?;
        if let Some(start) = start {
            let end = start
                .checked_add_signed(duration)
                .ok_or_else(|| anyhow::format_err!("failed to compute 'end' time"))?;
            return Ok((start, end));
        }
        if let Some(end) = end {
            let start = end
                .checked_sub_signed(duration)
                .ok_or_else(|| anyhow::format_err!("failed to compute 'start' time"))?;
            return Ok((start, end));
        }
    }
    if start.is_none() || end.is_none() {
        return Err(anyhow::format_err!("'start' and 'end' expected"));
    }
    Ok((start.unwrap(), end.unwrap()))
}

pub fn get_duration(q: &TimeRangeOpts) -> anyhow::Result<(NaiveDateTime, NaiveDateTime)> {
    get_duration_helper(q.start, q.end, q.duration, q.since)
}

#[derive(Parser, Debug)]
/// loki misc apis
pub struct QueryMisc {
    #[command(flatten)]
    http: HttpOpts,

    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Parser, Debug)]
enum SubCommand {
    /// query labels set
    #[clap(aliases=&["l", "la"])]
    Labels(LabelsCommand),

    /// query label values
    #[clap(aliases=&["lv"])]
    LabelValues(LabelValuesCommand),
}

#[derive(Parser, Debug)]
struct LabelsCommand {
    #[command(flatten)]
    time_range: TimeRangeOpts,
}

#[derive(Parser, Debug)]
struct LabelValuesCommand {
    #[command(flatten)]
    time_range: TimeRangeOpts,

    /// label name
    label: String,
}

#[derive(Debug, Serialize)]
struct LabelsReq {
    start: Option<i64>,
    end: Option<i64>,
}

pub(crate) fn query_misc(q: QueryMisc) -> anyhow::Result<()> {
    let req = match q.cmd {
        SubCommand::Labels(l) => {
            let client = reqwest::blocking::Client::new();
            let req = client.get(format!("{}/loki/api/v1/labels", q.http.endpoint));
            let req = refine_loki_request(req, q.http.headers, q.http.basic_auth, q.http.tenant);
            let (start, end) = match get_duration(&l.time_range) {
                Ok(r) => {
                    debug!("start: {}, end: {}", r.0, r.1);
                    (Some(r.0.timestamp_nanos()), Some(r.1.timestamp_nanos()))
                },
                Err(err) => {
                    debug!("{}", err);
                    (None, None)
                }
            };
            debug!("start: {start:?}, end: {end:?}");
            req.query(&LabelsReq{
                start,
                end,
            })
        }
        SubCommand::LabelValues(lv) => {
            let client = reqwest::blocking::Client::new();
            let req = client.get(format!("{}/loki/api/v1/label/{}/values", q.http.endpoint, lv.label));
            let req = refine_loki_request(req, q.http.headers, q.http.basic_auth, q.http.tenant);
            let (start, end) = match get_duration(&lv.time_range) {
                Ok(r) => {
                    debug!("start: {}, end: {}", r.0, r.1);
                    (Some(r.0.timestamp_nanos()), Some(r.1.timestamp_nanos()))
                }
                Err(err) => {
                    debug!("{}", err);
                    (None, None)
                }
            };
            debug!("start: {start:?}, end: {end:?}");
            req.query(&LabelsReq{
                start,
                end,
            })
        },
    };
    let resp = req.send()?;
    println!("{}", resp.status());
    let obj: serde_json::Value = serde_json::from_str(&resp.text()?)?;
    println!("{}", serde_json::to_string_pretty(&obj)?);
    Ok(())
}

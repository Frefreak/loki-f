use std::{
    cmp::{max, min},
    collections::HashSet,
    str::from_utf8,
};

use anyhow::Result;
use base64::{encode_config, STANDARD_NO_PAD};
use chrono::{Local, NaiveDateTime};
use clap::Parser;
use nut::DBBuilder;
use ring::digest::{digest, SHA256};

use crate::{
    common::{blue, gray, green, yellow, KeyValue, TimeRangeOpts, red},
    query::get_duration,
};

/// boltdb inspection (based on loki v2.6.1)
#[derive(Parser, Debug)]
pub struct Bolt {
    #[command(flatten)]
    time_range: TimeRangeOpts,

    /// query label string
    #[arg(short, long, num_args=1..)]
    query: Vec<KeyValue>,

    /// boltdb file
    file: String,

    /// tenant name
    #[arg(short, long, default_value = "fake")]
    tenant: String,

    /// row shard
    #[arg(short, long, default_value = "16")]
    shard: u32,

    /// disable broad queries
    #[arg(long)]
    disable_broad_queries: bool,
}

pub fn inspect(b: Bolt) -> Result<()> {
    println!("To simplify things, we assume a few things:");
    println!("  1. schema is 24 hour, making bucket size 86400000, also v11 is used");
    println!(
        "  2. we only consider MatchEqual exprs, so query only accepts something like a=1 b=2"
    );
    println!("{}", yellow("we now begin\n"));

    let (buckets, (start, end)) = get_buckets(&b);
    let mut series_ids = HashSet::default();
    let db = DBBuilder::new(b.file.clone()).read_only(true).build()?;
    let tx = db.begin_tx()?;
    let bucket = tx.bucket(b"index")?;
    for kv in b.query.iter() {
        println!("{:?}", kv);
        let queries = calc_queries(b.shard, &buckets, kv);

        println!("\n{}", gray("getting entries (query pages)..."));
        let entries = get_entries_from_queries(b.disable_broad_queries, &bucket, queries)?;

        println!("len: {}", entries.len());
        for entry in entries.iter() {
            println!("{:?}", entry);
        }

        println!("\n{}", gray("parsing index entries"));
        let batch_result: Vec<_> = entries
            .iter()
            .map(|e| parse_chunk_time_range_value(&e.range_value))
            .collect::<anyhow::Result<_>>()?;

        print!("{}", gray("len of batch result: "));
        println!("{}", batch_result.len());
        print!("{}", gray("after dedup: "));
        let unique_set: HashSet<String> = batch_result.into_iter().collect();
        println!("{}", unique_set.len());
        println!("batch series ids for {:?}: {:?}", kv, unique_set);

        if series_ids.is_empty() {
            series_ids = unique_set;
        } else {
            let t = series_ids.intersection(&unique_set).collect::<HashSet<_>>();
            series_ids = t.into_iter().cloned().collect();
        }
    }
    let result: Vec<_> = series_ids.into_iter().collect();
    println!("{}", red(&format!("final series_ids: {:?}", result)));

    println!("\n{}", gray("make new queries based on series id (v10)"));
    let queries = calc_queries_for_serires(&buckets, result);
    print!("{}", gray("len: "));
    println!("{}", queries.len());
    println!("{:?}", queries);

    // this time will definitely go to the broad query route
    let entries = get_entries_from_queries(false, &bucket, queries)?;
    print!("{}: ", gray("entries by series id"));
    println!("{}\n{:?}", entries.len(), entries);

    println!("\n{}", gray("parsing index entries, again"));

    let result: Vec<_> = entries
        .iter()
        .map(|e| parse_chunk_time_range_value(&e.range_value))
        .collect::<anyhow::Result<_>>()?;
    println!("got chunk-ids:\n{:?}", result);
    println!("len: {}", result.len());

    let mut chunk_refs = vec![];
    for r in result {
        let mut rsp = r.split("/");
        let tenant_id = rsp.next().unwrap();
        let segs = rsp.next().unwrap();
        let parts = segs.split(":").collect::<Vec<_>>();
        let fingerprint = u64::from_str_radix(parts[0], 16)?;
        let from = i64::from_str_radix(parts[1], 16)?;
        let to = i64::from_str_radix(parts[2], 16)?;
        let checksum = u32::from_str_radix(parts[3], 16)?;
        if to < start.timestamp_millis() || from > end.timestamp_millis() {
            continue;
        }
        chunk_refs.push(ChunkRef {
            user_id: tenant_id.to_string(),
            fingerprint,
            from,
            to,
            checksum,
        });
    }
    println!("final result:\n{:?}", chunk_refs);
    println!("len: {}", chunk_refs.len());
    Ok(())
}

// only do match_equal
fn filter_entries(entries: &Vec<Entry>, query: &Query) -> Vec<Entry> {
    entries.into_iter().filter(|x| {
        if query.range_value_prefix.len() > 0 && !x.range_value.starts_with(&query.range_value_prefix) {
            return false;
        }
        // I compared with loki's implementation, this can only filter out "some" chunk
        // if the time starts with 00000000 this won't be able to filter out any chunk
        // we need additional filter for time range
        // TODO: pkg/storage/chunk/chunk.go
        if query.range_value_start.len() > 0 && query.range_value_start > x.range_value {
            return false;
        }
        if query.value_equal.len() > 0 && query.value_equal != x.value {
            return false;
        }
        return true;
    }).cloned().collect()
}

#[derive(Debug)]
#[allow(dead_code)]
struct Bucket {
    from: u32,
    through: u32,
    table_name: String,
    hash_key: String,
    bucket_size: u32,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Query {
    table_name: String,
    hash_value: String,
    range_value_prefix: String,
    range_value_start: String,
    value_equal: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Entry {
    table_name: String,
    hash_value: String,
    range_value: String,
    value: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ChunkRef {
    user_id: String,
    fingerprint: u64,
    from: i64,
    to: i64,
    checksum: u32,
}

fn get_buckets(b: &Bolt) -> (Vec<Bucket>, (NaiveDateTime, NaiveDateTime)) {
    println!("{}", gray("calculating start/end..."));
    let (start, end) = match get_duration(&b.time_range) {
        Ok(k) => {
            println!("determined given time range: ");
            k
        }
        Err(_) => {
            println!("failed to determined given time range, using default (past 1 hour): ");
            let end = Local::now().naive_utc();
            let start = end.checked_sub_signed(chrono::Duration::hours(1)).unwrap();
            (start, end)
        }
    };

    println!(
        "start: {}, end: {}",
        green(&start.to_string()),
        green(&end.to_string())
    );

    println!("\n{}", gray("preparing 'Buckets'..."));
    let mut buckets = vec![];
    let from_day = start.timestamp() / 86400;
    let to_day = end.timestamp() / 86400;
    for d in from_day..=to_day {
        let relative_from = max(0, start.timestamp_millis() - d * 86_400_000);
        let relative_through = min(86_400_000, end.timestamp_millis() - d * 86_400_000);
        buckets.push(Bucket {
            from: relative_from as u32,
            through: relative_through as u32,
            table_name: format!("index_{}", d),
            hash_key: format!("{}:d{}", b.tenant, d),
            bucket_size: 86_400_000,
        });
    }
    println!("{:#?}", buckets);
    (buckets, (start, end))
}

fn calc_queries(shard: u32, buckets: &Vec<Bucket>, kv: &KeyValue) -> Vec<Query> {
    let mut queries = vec![];
    for bucket in buckets.iter() {
        println!(
            "{}, {}",
            blue(&format!("{:?}", kv)),
            yellow(&format!("{:?}", bucket))
        );
        let hash_val = digest(&SHA256, kv.value.as_ref());
        let mut hash_val_encoded = encode_config(hash_val, STANDARD_NO_PAD);
        hash_val_encoded.push_str("\x00");
        for i in 0..shard {
            queries.push(Query {
                table_name: bucket.table_name.clone(),
                hash_value: format!("{:02}:{}:logs:{}", i, bucket.hash_key, kv.key),
                range_value_prefix: hash_val_encoded.clone(),
                range_value_start: String::default(),
                value_equal: kv.value.clone(),
            });
        }
    }
    println!("len: {}", queries.len());
    for query in queries.iter() {
        println!("{:?}", query);
    }
    queries
}

// Returns the chunkID (seriesID since v9) and labelValue for chunk time
// range values.
// Orig implementation is at: pkg/storage/stores/series/index/schema_util.go
// Note: this is just a partial implementation, which only targets for schema
// version v11 and only returns chunk_id.
fn parse_chunk_time_range_value(range_value: &String) -> anyhow::Result<String> {
    let components = range_value.split("\x00").collect::<Vec<_>>();
    if components.len() != 5 {
        return Err(anyhow::format_err!(
            "components lens: {}, should be 5",
            components.len()
        ));
    }
    match components[3] {
        "3" => {
            return Ok(components[2].to_string());
        }
        "8" => {
            return Ok(components[1].to_string());
        }
        other => {
            return Err(anyhow::format_err!(
                "components[3] has unexpected value: {}",
                other
            ));
        }
    }
}

fn do_broad_queries(bucket: &nut::Bucket, queries: Vec<Query>) -> anyhow::Result<Vec<Entry>> {
    let queries = queries.into_iter().map(|q| Query {
        table_name: q.table_name,
        hash_value: q.hash_value,
        range_value_prefix: String::default(),
        range_value_start: q.range_value_start,
        value_equal: q.value_equal,
    }).collect();
    query_pages(bucket, queries)
}

// Returns entries from queries.
// Orig implementation is roughly at:
// - pkg/storage/chunk/client/local/boltdb_index_client.go nextItem
// - pkg/storage/stores/series/index/caching_index_client.go doBroadQueries/doQueries?
// Only the simple case MatchEqual is implemented
fn get_entries_from_queries(
    disable_broad_queries: bool,
    bucket: &nut::Bucket,
    queries: Vec<Query>,
) -> anyhow::Result<Vec<Entry>> {
    if !disable_broad_queries {
        do_broad_queries(bucket, queries)
    } else {
        query_pages(bucket, queries)
    }
}

fn query_pages(
    bucket: &nut::Bucket,
    queries: Vec<Query>,
) -> anyhow::Result<Vec<Entry>> {
    let mut entries = vec![];
    for query in queries {
        let prefix_len = query.hash_value.len() + 1;
        let start = if query.range_value_prefix.len() > 0 {
            query.hash_value.clone() + "\x00" + &query.range_value_prefix
        } else if query.range_value_start.len() > 0 {
            // query.hash_value + "\x00" + &query.range_value_start
            // original code appends range_value_start here
            // but doesn't actually use it in iterator to filter
            query.hash_value.clone() + "\x00"
        } else {
            query.hash_value.clone() + "\x00"
        };
        let mut sub_entries = vec![];
        bucket.for_each(Box::new(|key, value| -> Result<(), String> {
            if key.starts_with(start.as_bytes()) {
                if value.is_none() {
                    return Ok(());
                } else {
                    if query.value_equal.len() > 0 {
                        if value.unwrap() != query.value_equal.as_bytes() {
                            return Ok(())
                        }
                    }
                }
                let range_value = from_utf8(&key[prefix_len..]).unwrap().to_string();
                sub_entries.push(Entry {
                    table_name: query.table_name.clone(),
                    hash_value: start.clone(),
                    range_value,
                    value: from_utf8(value.unwrap()).unwrap().to_string(),
                });
            }
            Ok(())
        }))?;
        entries.extend(filter_entries(&sub_entries, &query));
    }
    return Ok(entries);
}

fn calc_queries_for_serires(buckets: &Vec<Bucket>, series_ids: Vec<String>) -> Vec<Query> {
    println!("\n{}", gray("make Query for series id"));
    let mut queries = vec![];
    for bucket in buckets {
        queries.extend(series_ids.iter().map(|id| {
            let encode_from_bytes = encode_time(bucket.from);
            Query {
                table_name: bucket.table_name.clone(),
                hash_value: format!("{}:{}", bucket.hash_key, id),
                range_value_prefix: String::default(),
                range_value_start: encode_from_bytes,
                value_equal: String::default(),
            }
        }))
    }
    queries
}

// just a big endian hex representation for u32
fn encode_time(from: u32) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}",
        (from & 0xff000000) >> 24,
        (from & 0x00ff0000) >> 16,
        (from & 0x0000ff00) >> 8,
        (from & 0x000000ff)
    )
}

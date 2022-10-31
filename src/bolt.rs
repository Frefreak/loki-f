use anyhow::Result;
use clap::Parser;
use nut::DBBuilder;

/// boltdb inspection
#[derive(Parser, Debug)]
pub struct Bolt {
    /// labels to use, "prog=lf" if not given
    file: String,
}

pub fn inspect(b: Bolt) -> Result<()> {
    let db = DBBuilder::new(b.file).build()?;
    let tx = db.begin_tx()?;
    let buckets = tx.buckets();
    for bucket in buckets {
        println!("{:?}", bucket);
        let bucket = tx.bucket(&bucket)?;
        println!("{:?}", bucket.stats());
    }
    Ok(())
}

use std::{io::{stdout, Write, BufWriter}, fs::File};

use clap::{Parser, AppSettings};
use decode::decode_file;
use tracing::{debug, info};

pub mod ty;
pub mod decode;
pub mod push;
pub mod bolt;

#[derive(Parser, Debug)]
#[clap(version = "1.0")]
/// Loki How
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(subcommand)]
    command: SubCommand,
}

#[derive(Parser, Debug)]
enum SubCommand {
    /// decode chunk
    #[clap(aliases=&["d", "de", "dec"])]
    Decode(decode::Decode),

    /// push to loki
    #[clap(aliases=&["p", "push"])]
    Push(push::Push),

    /// boltdb inspection
    #[clap(aliases=&["b", "boltdb"])]
    Bolt(bolt::Bolt),
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let opts = Opts::parse();
    match opts.command {
        SubCommand::Decode(d) => {
            debug!("{d:?}");
            let chunk = decode_file(d.input)?;
            if d.noout {
                return Ok(());
            }
            info!("{:?}", chunk.data.meta);
            let writer: Box<dyn Write> = if d.output == "-" {
                Box::new(BufWriter::new(stdout().lock()))
            } else {
                Box::new(BufWriter::new(File::create(d.output)?))
            };
            if d.compact {
                serde_json::to_writer(writer, &chunk)?;
            } else {
                serde_json::to_writer_pretty(writer, &chunk)?;
            }
            Ok(())
        },
        SubCommand::Push(p) => {
            push::push(p)?;
            Ok(())
        },
        SubCommand::Bolt(b) => {
            bolt::inspect(b)?;
            Ok(())
        },
    }
}

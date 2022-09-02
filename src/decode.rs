use std::{io::{Read, Seek, Cursor}, path::Path};

use binread::BinReaderExt;
use clap::Parser;

use crate::ty::Chunk;

/// decode proto struct from input
#[derive(Parser, Debug)]
pub struct Decode {
    /// input file (binary input)
    #[clap(short, long)]
    pub input: String,

    /// output file (json output)
    #[clap(short, long, default_value="out.json")]
    pub output: String,

    /// disable pretty output
    #[clap(short, long)]
    pub compact: bool,
}

fn decode_chunk<R: Read + Seek>(reader: &mut R) -> anyhow::Result<Chunk> {
    match reader.read_le() {
        Ok(chunk) => Ok(chunk),
        Err(error) => {
            match error {
                binread::Error::Custom { pos: _, err: _ } => {
                    let err_msg = error.custom_err::<anyhow::Error>().unwrap();
                    Err(anyhow::format_err!("{err_msg:?}"))
                }
                err => {
                    Err(anyhow::format_err!("{err}"))
                }
            }
        }
    }
}

pub fn decode_file<P: AsRef<Path>>(file: P) -> anyhow::Result<Chunk> {
    let bs = std::fs::read(file).unwrap();
    let mut cursor = Cursor::new(bs);
    decode_chunk(&mut cursor)
}

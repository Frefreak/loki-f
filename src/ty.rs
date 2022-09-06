use std::{
    collections::HashMap,
    io::{Cursor, Read},
};

use binread::{error::magic, BinRead, BinReaderExt, BinResult, Endian};
use chrono::NaiveDateTime;
use flate2::read::GzDecoder;
use integer_encoding::VarIntReader;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Debug, Clone, Serialize)]
pub struct UnorderedBlock {
    pub entries: Vec<UnorderedBlockEntry>,
}

// loki/pkg/chunkenc/unordered.go Serialise
#[derive(Debug, Clone, Serialize)]
pub struct UnorderedBlockEntry {
    pub time: NaiveDateTime,
    pub line: String,
}

impl BinRead for UnorderedBlockEntry {
    type Args = ();

    fn read_options<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
        _options: &binread::ReadOptions,
        _args: Self::Args,
    ) -> binread::BinResult<Self> {
        let ts = reader.read_varint::<i64>()?;
        let sz = reader.read_varint::<u64>()?;
        let mut vec = vec![0; sz as usize];
        reader.read_exact(vec.as_mut())?;
        Ok(UnorderedBlockEntry {
            time: NaiveDateTime::from_timestamp(ts / (1e9 as i64), 0),
            line: String::from_utf8_lossy(&vec).to_string(),
        })
    }
}

impl BinRead for UnorderedBlock {
    type Args = usize;

    fn read_options<R: Read + std::io::Seek>(
        reader: &mut R,
        _options: &binread::ReadOptions,
        args: Self::Args,
    ) -> BinResult<Self> {
        let mut entries = vec![];
        for _ in 0..args {
            let entry = reader.read_le()?;
            entries.push(entry);
        }
        debug!("pos after parsing {}", reader.stream_position()?);
        Ok(UnorderedBlock { entries })
    }
}

// loki/pkg/chunkenc/memchunk.go WriteTo
#[derive(Debug, Clone, Serialize)]
pub struct BlockMeta {
    pub num_entries: usize,
    pub mint: NaiveDateTime,
    pub maxt: NaiveDateTime,
    pub offset: u64,
    // chunk format v3
    pub uncompressed_size: usize,
    pub compressed_size: usize,
}

impl BinRead for BlockMeta {
    type Args = ();

    fn read_options<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
        _options: &binread::ReadOptions,
        _args: Self::Args,
    ) -> binread::BinResult<Self> {
        let num_entries = reader.read_varint()?;
        let mint = reader.read_varint::<i64>()?;
        let maxt = reader.read_varint::<i64>()?;
        let offset = reader.read_varint()?;
        let uncompressed_size = reader.read_varint()?;
        let compressed_size = reader.read_varint()?;
        Ok(BlockMeta {
            num_entries,
            mint: NaiveDateTime::from_timestamp(mint / (1e9 as i64), 0),
            maxt: NaiveDateTime::from_timestamp(maxt / (1e9 as i64), 0),
            offset,
            uncompressed_size,
            compressed_size,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Meta {
    pub num_blocks: usize,
    pub block_metas: Vec<BlockMeta>,
    pub block_crc: u32,
}

impl BinRead for Meta {
    type Args = ();

    fn read_options<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
        _options: &binread::ReadOptions,
        _args: Self::Args,
    ) -> binread::BinResult<Self> {
        let num_blocks = reader.read_varint()?;
        let block_metas = (0..num_blocks)
            .map(|_| reader.read_le())
            .collect::<BinResult<_>>()?;
        let crc32 = reader.read_le()?;
        //TODO: CRC check

        Ok(Meta {
            num_blocks,
            block_metas,
            block_crc: crc32,
        })
    }
}

#[repr(u8)]
#[derive(FromPrimitive, Debug, Clone, Serialize)]
pub enum EncType {
    EncNone,
    EncGZIP,
    EncDumb,
    EncLZ4_64k,
    EncSnappy,
    EncLZ4_256k,
    EncLZ4_1M,
    EncLZ4_4M,
    EncFlate,
    EncZstd,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChunkData {
    pub ty: EncType,
    pub blocks: Vec<UnorderedBlock>,
    pub meta: Meta,
}

impl BinRead for ChunkData {
    // data start offset
    type Args = ();

    fn read_options<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
        options: &binread::ReadOptions,
        _args: Self::Args,
    ) -> binread::BinResult<Self> {
        // skip length
        _ = reader.read_le::<u32>();

        let cur_pos = reader.stream_position()?;
        debug!("cur pos: {cur_pos}");
        reader.seek(std::io::SeekFrom::End(-8))?;
        let offset = reader.read_be::<u64>()?;
        debug!("offset: {offset}");
        reader.seek(std::io::SeekFrom::Start(offset + cur_pos))?;
        let meta: Meta = reader.read_le()?;
        debug!("meta parsed: {:?}", meta);

        reader.seek(std::io::SeekFrom::Start(cur_pos))?;
        let mut new_opt = *options;
        new_opt.endian = Endian::Big;
        debug!("finding magic 0x012ee56a");
        magic(reader, 0x012EE56A_u32, &new_opt)?;
        debug!("finding magic 3");
        magic(reader, 3_u8, &new_opt)?;
        let et = reader.read_le()?;
        let enc_type = EncType::from_u8(et).expect("invalid enc type");

        let mut blocks = vec![];
        for i in 0..meta.num_blocks {
            let block_meta = &meta.block_metas[i];
            reader.seek(std::io::SeekFrom::Start(block_meta.offset + cur_pos))?;
            let mut vec = vec![0; block_meta.compressed_size];

            debug!("uncompressed size: {}", block_meta.uncompressed_size);
            reader.read_exact(&mut vec)?;
            let bs = decompress(&vec, &enc_type, block_meta.num_entries)?;
            // assert_eq!(bs.line.len(), block_meta.uncompressed_size)
            blocks.push(bs);
        }

        Ok(ChunkData {
            ty: enc_type,
            blocks,
            meta,
        })
    }
}

// decompress chunk data (assumes unordered block)
fn decompress(vec: &[u8], enc_type: &EncType, num_entries: usize) -> BinResult<UnorderedBlock> {
    // std::fs::write("debug.bin", vec)?;
    debug!(
        "decompress called, vec len: {}, enc type: {:?}",
        vec.len(),
        enc_type
    );
    // let vec = BufReader::new(vec);
    let decoded = match enc_type {
        EncType::EncGZIP => {
            let mut d = GzDecoder::new(vec);
            let mut s = Vec::new();
            d.read_to_end(&mut s)?;
            s
        }
        EncType::EncSnappy => {
            let mut decoder = snap::read::FrameDecoder::new(vec);
            let mut s = Vec::new();
            decoder.read_to_end(&mut s)?;
            s
        }
        EncType::EncZstd => {
            let mut decoder = zstd::Decoder::new(vec)?;
            let mut s = Vec::new();
            decoder.read_to_end(&mut s)?;
            s
        }
        e => {
            return Err(binread::Error::Custom {
                pos: 0,
                err: Box::new(anyhow::format_err!("not supported: {e:?}")),
            })
        }
    };
    debug!("real uncompressed size: {}", decoded.len());
    let mut cursor = Cursor::new(decoded);
    let unordered_block = cursor.read_le_args(num_entries)?;
    Ok(unordered_block)
}

// loki/pkg/storage/chunk/chunk.go Chunk
#[derive(Serialize)]
pub struct Chunk {
    pub header: ChunkHead,
    pub data: ChunkData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkHead {
    pub fingerprint: u64,
    #[serde(rename = "userID")]
    pub user_id: String,
    pub from: f64,
    pub through: f64,
    pub metric: HashMap<String, String>,
    pub encoding: u8,
}

impl BinRead for ChunkHead {
    type Args = ();

    fn read_options<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
        _options: &binread::ReadOptions,
        _args: Self::Args,
    ) -> binread::BinResult<Self> {
        let mut decoder = snap::read::FrameDecoder::new(reader);
        let mut s = Vec::new();
        decoder.read_to_end(&mut s)?;
        match serde_json::from_slice(&s) {
            Ok(h) => Ok(h),
            Err(err) => {
                println!("{:?}", err);
                Err(binread::Error::Custom {
                    pos: 0,
                    err: Box::new(anyhow::format_err!("header json deserialize: {err:?}")),
                })
            }
        }
    }
}

impl BinRead for Chunk {
    type Args = ();

    fn read_options<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
        _options: &binread::ReadOptions,
        _args: Self::Args,
    ) -> binread::BinResult<Self> {
        let head_sz = reader.read_be::<u32>()? as usize;
        let mut vec = vec![0; head_sz - 4];
        reader.read_exact(&mut vec)?;
        let mut cursor = Cursor::new(vec);
        let header = cursor.read_le()?;
        println!("{:?}", header);
        let data = reader.read_le()?;
        Ok(Chunk { header, data })
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use binread::BinRead;

    use crate::ty::{ChunkData, ChunkHead, Meta};

    use super::{BlockMeta, UnorderedBlockEntry};

    #[test]
    fn test_parse_unordered_block() -> anyhow::Result<()> {
        let mut cursor = Cursor::new(&[
            128, 200, 152, 153, 191, 238, 181, 144, 46, 8, 102, 105, 122, 122, 98, 117, 122, 122,
        ]);

        let blk: UnorderedBlockEntry = BinRead::read(&mut cursor)?;
        assert_eq!(format!("{:?}", blk.time), "2022-08-31T11:51:49");
        assert_eq!(blk.line, "fizzbuzz");
        Ok(())
    }

    #[test]
    fn test_parse_block_meta() -> anyhow::Result<()> {
        let mut cursor = Cursor::new(&[
            1, 128, 200, 152, 153, 191, 238, 181, 144, 46, 128, 200, 152, 153, 191, 238, 181, 144,
            46, 6, 8, 43,
        ]);

        let meta: BlockMeta = BinRead::read(&mut cursor)?;
        assert_eq!(meta.num_entries, 1);
        assert_eq!(format!("{:?}", meta.mint), "2022-08-31T11:51:49");
        assert_eq!(format!("{:?}", meta.maxt), "2022-08-31T11:51:49");
        assert_eq!(meta.offset, 6);
        assert_eq!(meta.uncompressed_size, 8);
        assert_eq!(meta.compressed_size, 43);
        Ok(())
    }

    #[test]
    fn test_parse_meta() -> anyhow::Result<()> {
        let mut cursor = Cursor::new(&[
            1, 1, 128, 200, 152, 153, 191, 238, 181, 144, 46, 128, 200, 152, 153, 191, 238, 181,
            144, 46, 6, 8, 43, 199, 132, 40, 177,
        ]);

        let meta: Meta = BinRead::read(&mut cursor)?;
        assert_eq!(meta.num_blocks, 1);
        assert_eq!(meta.block_metas.len(), 1);
        assert_eq!(meta.block_crc, 2972222663);
        Ok(())
    }

    #[test]
    fn test_parse_chunk_data() -> anyhow::Result<()> {
        // first 4 bytes are size field
        let mut cursor = Cursor::new(&[
            0, 0, 0, 0, 1, 46, 229, 106, 3, 1, 31, 139, 8, 0, 0, 9, 110, 136, 0, 255, 0, 18, 0,
            237, 255, 128, 200, 152, 153, 191, 238, 181, 144, 46, 8, 102, 105, 122, 122, 98, 117,
            122, 122, 3, 0, 220, 180, 200, 63, 18, 0, 0, 0, 180, 135, 149, 161, 1, 1, 128, 200,
            152, 153, 191, 238, 181, 144, 46, 128, 200, 152, 153, 191, 238, 181, 144, 46, 6, 8, 43,
            199, 132, 40, 177, 0, 0, 0, 0, 0, 0, 0, 53,
        ]);

        let ch: ChunkData = BinRead::read(&mut cursor)?;
        println!("{:?}", ch);
        Ok(())
    }

    #[test]
    fn test_parse_chunk_head() -> anyhow::Result<()> {
        let mut cursor = Cursor::new(&[
            255, 6, 0, 0, 115, 78, 97, 80, 112, 89, 1, 202, 0, 0, 119, 243, 141, 142, 123, 34, 102,
            105, 110, 103, 101, 114, 112, 114, 105, 110, 116, 34, 58, 49, 49, 53, 56, 49, 52, 49,
            52, 56, 53, 50, 53, 55, 57, 53, 53, 50, 49, 55, 44, 34, 117, 115, 101, 114, 73, 68, 34,
            58, 34, 98, 97, 114, 34, 44, 34, 102, 114, 111, 109, 34, 58, 49, 54, 54, 49, 57, 53,
            49, 49, 48, 52, 46, 50, 54, 52, 44, 34, 116, 104, 114, 111, 117, 103, 104, 34, 58, 49,
            54, 54, 49, 57, 53, 49, 50, 51, 56, 46, 53, 50, 50, 44, 34, 109, 101, 116, 114, 105,
            99, 34, 58, 123, 34, 95, 95, 110, 97, 109, 101, 95, 95, 34, 58, 34, 108, 111, 103, 115,
            34, 44, 34, 97, 99, 116, 95, 105, 100, 34, 58, 34, 49, 48, 48, 56, 56, 34, 44, 34, 99,
            97, 116, 101, 103, 111, 114, 121, 34, 58, 34, 98, 101, 110, 99, 104, 34, 44, 34, 99,
            111, 109, 112, 111, 110, 101, 110, 116, 34, 58, 34, 86, 111, 114, 117, 120, 34, 125,
            44, 34, 101, 110, 99, 111, 100, 105, 110, 103, 34, 58, 49, 50, 57, 125, 10,
        ]);

        let head: ChunkHead = BinRead::read(&mut cursor)?;
        assert_eq!(head.metric.len(), 4);
        Ok(())
    }
}

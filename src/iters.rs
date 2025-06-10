use std::{
    fs::File,
    io::{BufReader, Read},
    marker::PhantomData,
    path::Path,
};

use anyhow::Result;
use opentelemetry_proto::tonic::{logs::v1::LogsData, trace::v1::TracesData};
use prost::Message;
use zstd::stream::Decoder;

pub struct ChunkIter<T> {
    reader: Option<BufReader<File>>,
    buf: Vec<u8>,
    _msg: PhantomData<T>,
}

pub type TracesIter = ChunkIter<TracesData>;
pub type LogsIter = ChunkIter<LogsData>;

impl<T> ChunkIter<T>
where
    T: Message + Default,
{
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Ok(ChunkIter {
            reader: Some(BufReader::new(File::open(path)?)),
            buf: Vec::new(),
            _msg: PhantomData,
        })
    }

    fn _next(&mut self) -> Result<Option<T>> {
        let mut reader = self.reader.take().expect("reader must exist");

        let size = {
            let mut size_as_bytes = [0u8; 4];
            if let Err(e) = reader.read_exact(&mut size_as_bytes) {
                if matches!(e.kind(), std::io::ErrorKind::UnexpectedEof) {
                    return Ok(None);
                }
                return Err(e.into());
            }
            u32::from_be_bytes(size_as_bytes)
        };

        let mut decoder = Decoder::with_buffer(reader.take(size as u64))?;
        decoder.read_to_end(&mut self.buf)?;
        self.reader = Some(decoder.finish().into_inner());
        Ok(Some(T::decode(&self.buf[..])?))
    }
}

impl<T> Iterator for ChunkIter<T>
where
    T: Message + Default,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self._next().transpose()
    }
}

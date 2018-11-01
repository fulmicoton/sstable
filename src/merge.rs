use SSTable;
use std::io;
use super::Reader;
use std::collections::BinaryHeap;
use Writer;

pub fn merge<SST: SSTable, R: io::BufRead, W: io::Write>(io_readers: Vec<R>, write: W) -> io::Result<()> {
    // let readers:
    let mut readers = vec![];
    for mut reader in io_readers.into_iter().map(SST::reader) {
        if reader.advance()? {
            readers.push(reader)
        }
    }
    let writer = SST::writer(write);
    Ok(())
}

pub fn merge_sstable<SST: SSTable, R: io::BufRead, W: io::Write>(
    readers: Vec<Reader<R, SST::Reader>>,
    mut writer: Writer<W, SST::Writer>) -> io::Result<()> {
    unimplemented!();
}

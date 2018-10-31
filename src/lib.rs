extern crate slice_deque;

use std::io::{self, Write, BufWriter};
use std::mem;

pub(crate) mod buffer;
pub(crate) mod vint;
pub mod value;

use self::buffer::Buffer;

const DEFAULT_KEY_CAPACITY: usize = 50;
const FOUR_BIT_LIMITS: usize = 1 << 4;
const BUFFER_AHEAD: usize = 4_096;

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter().cloned()
        .zip(right.iter().cloned())
        .take_while(|(left, right)| left==right)
        .count()
}

pub trait SSTable {

    type Value;
    type Reader: value::ValueReader<Value=Self::Value>;
    type Writer: value::ValueWriter<Value=Self::Value>;

    fn writer<W: io::Write>(writer: W) -> Writer<W, Self::Writer> {
        Writer {
            previous_key: Vec::with_capacity(DEFAULT_KEY_CAPACITY),
            write: BufWriter::new(writer),
            value_writer: Self::Writer::default()
        }
    }

    fn reader<R: io::Read>(data: R) -> Reader<R, Self::Reader> {
        Reader {
            data,
            key: Vec::with_capacity(DEFAULT_KEY_CAPACITY),
            value_reader: Self::Reader::default(),
            buffer: Buffer::new(),
            spare_buffer: Buffer::new(),
        }
    }
}

pub struct VoidSSTable;

impl SSTable for VoidSSTable {
    type Value = ();
    type Reader = value::VoidReader;
    type Writer = value::VoidWriter;
}

pub struct Reader<R, TValueReader> {
    data: R,
    key: Vec<u8>,
    value_reader: TValueReader,
    buffer: Buffer,
    spare_buffer: Buffer,
}

impl<R,TValueReader> Reader<R,TValueReader>
    where R: io::Read, TValueReader: value::ValueReader {
    fn read_key(&mut self) -> io::Result<bool> {
        let b: u8 = self.buffer.pop_byte();
        let keep: usize;
        let add: usize;
        if b == 0 {
            keep = self.buffer.deserialize_u64() as usize;
            if keep == 0 {
                return Ok(false);
            }
            add = self.buffer.deserialize_u64() as usize;
        } else {
            keep = (b & 0b1111) as usize;
            add = (b >> 4) as usize;
        }
        self.key.resize(keep, 0u8);
        self.key.extend_from_slice(self.buffer.pop_slice(add));
        Ok(true)
    }

    fn fill_buffer(&mut self) -> io::Result<()> {
        self.spare_buffer.copy_from(&self.buffer);
        mem::swap(&mut self.spare_buffer, &mut self.buffer);
        self.buffer.fill(&mut self.data)
    }

    pub fn advance(&mut self) -> io::Result<bool> {
        if self.buffer.available() < BUFFER_AHEAD {
            self.fill_buffer()?;
        }
        if !self.read_key()? {
            return Ok(false);
        }
        self.value_reader.read(&mut self.data)?;
        Ok(true)
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &TValueReader::Value {
        self.value_reader.value()
    }
}


pub struct Writer<W, TValueWriter>
    where W: io::Write {
    previous_key: Vec<u8>,
    write: BufWriter<W>,
    value_writer: TValueWriter,
}

impl<W, TValueWriter> Writer<W, TValueWriter>
    where W: io::Write, TValueWriter: value::ValueWriter {

    fn encode_keep_add(&mut self, keep_len: usize, add_len: usize) -> io::Result<()> {
        if keep_len < FOUR_BIT_LIMITS && add_len < FOUR_BIT_LIMITS {
            let b = (keep_len | add_len << 4) as u8;
            self.write.write_all(&[b])
        } else {
            let mut buf = [0u8; 20];
            let mut len = vint::serialize(keep_len as u64, &mut buf[..]);
            len += vint::serialize(add_len as u64, &mut buf[len..]);
            self.write.write_all(&mut buf[..len])
        }
    }

    fn write(&mut self, key: &[u8], value: &TValueWriter::Value) -> io::Result<()> {
        let keep_len = common_prefix_len(&self.previous_key, key);
        let add_len = key.len() - keep_len;
        let increasing_keys =
            add_len > 0 &&
                (self.previous_key.len() == keep_len ||
                 self.previous_key[keep_len] < key[keep_len]);
        assert!(increasing_keys, "Keys should be increasing. ({:?} > {:?})", self.previous_key, key);
        let extension = &key[keep_len..];
        self.previous_key.resize(keep_len, 0u8);
        self.previous_key.extend_from_slice(extension);
        self.encode_keep_add(keep_len, add_len)?;
        self.write.write_all(extension)?;
        self.value_writer.write(value, &mut self.write)?;
        Ok(())
    }

    fn finalize(mut self) -> io::Result<()> {
        self.write.write(&[0u8, 0u8])?;
        self.write.flush()
    }
}


#[cfg(test)]
mod tests {
    use common_prefix_len;
    use super::VoidSSTable;
    use super::SSTable;

    fn aux_test_common_prefix_len(left: &str, right: &str, expect_len: usize) {
        assert_eq!(common_prefix_len(left.as_bytes(), right.as_bytes()), expect_len);
        assert_eq!(common_prefix_len(right.as_bytes(), left.as_bytes()), expect_len);
    }

    #[test]
    fn test_common_prefix_len() {
        aux_test_common_prefix_len("a", "ab", 1);
        aux_test_common_prefix_len("", "ab", 0);
        aux_test_common_prefix_len("ab", "abc", 2);
        aux_test_common_prefix_len("abde", "abce", 2);
    }

    #[test]
    fn test_simple_sstable() {
        let mut buffer = vec![];
        {
            let mut sstable_writer = VoidSSTable::writer(&mut buffer);
            assert!(sstable_writer.write(&[17u8], &()).is_ok());
            assert!(sstable_writer.write(&[17u8, 18u8, 19u8], &()).is_ok());
            assert!(sstable_writer.write(&[17u8, 20u8], &()).is_ok());
            assert!(sstable_writer.finalize().is_ok());
        }
        assert_eq!(&buffer, &[
            16u8, 17u8,
            33u8, 18u8, 19u8,
            17u8, 20u8,
            0u8, 0u8]);
        let mut sstable_reader = VoidSSTable::reader(&buffer[..]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[17u8]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[17u8, 18u8, 19u8]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[17u8, 20u8]);
        assert!(!sstable_reader.advance().unwrap());
    }


    #[test]
    #[should_panic]
    fn test_simple_sstable_non_increasing_key() {
        let mut buffer = vec![];
        let mut sstable_writer = VoidSSTable::writer(&mut buffer);
        assert!(sstable_writer.write(&[17u8], &()).is_ok());
        assert!(sstable_writer.write(&[16u8], &()).is_ok());
    }

}
use std::io;
use vint;

const BUFFER_LEN: usize = 4_096 * 10;




pub struct Buffer {
    buffer: Box<[u8; BUFFER_LEN]>,
    start: usize,
    stop: usize
}

impl Buffer {
    pub fn new() -> Buffer {
        Buffer {
            buffer: Box::new([0u8; BUFFER_LEN]),
            start: 0,
            stop: 0
        }
    }

    pub fn available(&self) -> usize {
        self.stop - self.start
    }

    pub fn pop_byte(&mut self) -> u8 {
        let b = self.buffer[self.start];
        self.start += 1;
        b
    }

    pub fn pop_slice(&mut self, len: usize) -> &[u8] {
        let start = self.start;
        self.start += len;
        &self.buffer[start..self.start]
    }

    pub fn deserialize_u64(&mut self) -> u64 {
        let (val, read_len)= vint::deserialize(&mut self.buffer[self.start..]);
        self.start += read_len;
        val
    }

    pub fn copy_from(&mut self, other: &Buffer) {
        self.start = 0;
        let len = other.available();
        self.buffer[0..len].copy_from_slice(other.as_ref());
        self.stop = len;
    }

    pub fn fill<R: io::Read>(&mut self, read: &mut R) -> io::Result<()> {
        loop {
            let space = &mut self.buffer[self.stop..];
            if space.is_empty() {
                break;
            }
            let n = read.read(space)?;
            if n == 0 {
                break;
            }
            self.stop += n;
        }
        Ok(())
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer[self.start..self.stop]
    }
}
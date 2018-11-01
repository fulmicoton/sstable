use std::io;

pub trait ValueReader: Default {

    type Value;

    fn value(&self) -> &Self::Value;

    fn read<R: io::BufRead>(&mut self, reader: &mut R) -> io::Result<()>;
}

pub trait ValueWriter: Default {

    type Value;

    fn write<W: io::Write>(&mut self, val: &Self::Value, writer: &mut W) -> io::Result<()>;
}


#[derive(Default)]
pub struct VoidReader;

impl ValueReader for VoidReader {
    type Value = ();

    fn value(&self) -> &Self::Value {
        &()
    }

    fn read<R: io::BufRead>(&mut self, _reader: &mut R) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct VoidWriter;

impl ValueWriter for VoidWriter {
    type Value = ();

    fn write<W: io::Write>(&mut self, _: &Self::Value, _: &mut W) -> Result<(), io::Error> {
        Ok(())
    }
}
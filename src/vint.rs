use std::io;

const CONTINUE_BIT: u8 = 128u8;

pub fn serialize(mut val: u64, buffer: &mut [u8]) -> usize {
    for (i, b) in buffer.iter_mut().enumerate() {
        let next_byte: u8 = (val & 127u64) as u8;
        val = val >> 7;
        if val == 0u64 {
            *b = next_byte;
            return i + 1;
        } else {
            *b = next_byte | CONTINUE_BIT;
        }
    }
    10 //< actually unreachable
}


// super slow but we don't care
pub fn deserialize_read<R: io::BufRead>(reader: &mut R) -> io::Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u64;
    let mut consumed = 0;
    'outer: loop {
        {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                return Err(From::from(io::ErrorKind::UnexpectedEof));
            }
            for &b in buf {
                consumed += 1;
                result |= u64::from(b % 128u8) << shift;
                if b < CONTINUE_BIT {
                    break 'outer;
                }
                shift += 7;
            }
        }
        reader.consume(consumed);
        consumed = 0;
    }
    reader.consume(consumed);
    Ok(result)
}


#[cfg(test)]
mod tests {
    use vint::serialize;
    use vint::deserialize_read;
    use std::u64;
    use std::io::{Read, BufReader};

    fn aux_test_int(val: u64, expect_len: usize) {
        let mut buffer = [0u8; 10];
        assert_eq!(serialize(val, &mut buffer[..]), expect_len);
        let mut r: &[u8] = &mut &buffer[..];
        let mut buf_reader = BufReader::new(&mut r);
        assert_eq!(deserialize_read(&mut buf_reader).unwrap(), val);
        let mut v = vec![];
        buf_reader.read_to_end(&mut v).unwrap();
        assert_eq!(expect_len + v.len(), buffer.len());
    }

    #[test]
    fn test_vint() {
        aux_test_int(0u64, 1);
        aux_test_int(17u64, 1);
        aux_test_int(127u64, 1);
        aux_test_int(128u64, 2);
        aux_test_int(123423418u64, 4);
        for i in 1..63 {
            let power_of_two = 1u64 << i;
            aux_test_int(power_of_two + 1, (i / 7) + 1);
            aux_test_int(power_of_two, (i / 7) + 1 );
            aux_test_int(power_of_two - 1, ((i-1) / 7) + 1);
        }
        aux_test_int(u64::MAX, 10);
    }
}


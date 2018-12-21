use BlockReader;

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
pub fn deserialize_read(block_reader: &mut BlockReader) -> u64 {
    let mut result = 0u64;
    let mut shift = 0u64;
    let mut consumed = 0;

    for &b in block_reader.buffer() {
        consumed += 1;
        result |= u64::from(b % 128u8) << shift;
        if b < CONTINUE_BIT {
            break;
        }
        shift += 7;
    }
    block_reader.consume(consumed);
    result
}


#[cfg(test)]
mod tests {
    use vint::serialize;
    use vint::deserialize_read;
    use std::u64;
    use BlockReader;
    use byteorder::{ByteOrder, LittleEndian};

    fn aux_test_int(val: u64, expect_len: usize) {
        let mut buffer = [0u8; 14];
        LittleEndian::write_u32(&mut buffer[..4], 10);
        assert_eq!(serialize(val, &mut buffer[4..]), expect_len);
        let r: &[u8] = &mut &buffer[..];
        let mut block_reader = BlockReader::new(Box::new(r));
        assert!(block_reader.read_block().unwrap());
        assert_eq!(deserialize_read(&mut block_reader), val);
        assert_eq!(expect_len + block_reader.buffer().len() + 4, buffer.len());
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


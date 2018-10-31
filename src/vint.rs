const CONTINUE_BIT: u8 = 128u8;

pub fn deserialize(bytes: &[u8]) -> (u64, usize) {
    let mut result = 0u64;
    let mut shift = 0u64;
    for (i, b) in bytes.iter().take(10).cloned().enumerate() {
        result |= u64::from(b & 127u8) << shift;
        if b < CONTINUE_BIT {
            return (result, i + 1);
        }
        shift += 7;
    }
    (0u64, 1)
}

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

#[cfg(test)]
mod tests {
    use vint::serialize;
    use vint::deserialize;
    use std::u64;

    fn aux_test_int(val: u64, expect_len: usize) {
        let mut buffer = [0u8; 10];
        assert_eq!(serialize(val, &mut buffer[..]), expect_len);
        assert_eq!(deserialize(&buffer[..]), (val, expect_len));
    }

    #[test]
    fn test_vint() {
        aux_test_int(0u64, 1);
        aux_test_int(17u64, 1);
        aux_test_int(127u64, 1);
        aux_test_int(128u64, 2);
        aux_test_int(1 << 14 - 1, 2);
        aux_test_int(1 << 14, 3);
        aux_test_int(u64::MAX, 10);
    }
}


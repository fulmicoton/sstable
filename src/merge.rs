use SSTable;
use std::io;
use super::Reader;
use std::collections::BinaryHeap;
use Writer;
use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::marker::PhantomData;


struct HeapItem<B: AsRef<[u8]>>(B);

impl<B: AsRef<[u8]>> Ord for HeapItem<B> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.as_ref().cmp(self.0.as_ref())
    }
}
impl<B: AsRef<[u8]>> PartialOrd for HeapItem<B> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(other.0.as_ref().cmp(self.0.as_ref()))
    }
}


pub struct VoidMerge;
impl ValueMerger<()> for VoidMerge {

    type TSingleValueMerger = ();

    fn new_value(&mut self, _: &()) -> () {
        ()
    }
}

impl SingleValueMerger<()> for () {
    fn add(&mut self, _: &()) {}

    fn flush(self) -> () {
        ()
    }
}

pub trait SingleValueMerger<V> {
    fn add(&mut self, v: &V);
    fn flush(self) -> V;
}

pub trait ValueMerger<V> {
    type TSingleValueMerger: SingleValueMerger<V>;
    fn new_value(&mut self, v: &V) -> Self::TSingleValueMerger;
}

#[derive(Default)]
pub struct KeepFirst<V> {
    _marker: PhantomData<V>,
}

pub struct FirstVal<V>(V);

impl<V: Clone> ValueMerger<V> for KeepFirst<V> {
    type TSingleValueMerger = FirstVal<V>;

    fn new_value(&mut self, v: &V) -> FirstVal<V> {
        FirstVal(v.clone())
    }
}

impl<V> SingleValueMerger<V> for FirstVal<V> {
    fn add(&mut self, _: &V) {}

    fn flush(self) -> V {
        self.0
    }
}


impl<B: AsRef<[u8]>> Eq for HeapItem<B> {}
impl<B: AsRef<[u8]>> PartialEq for HeapItem<B> {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

pub fn merge_sstable<SST: SSTable, R: io::BufRead, W: io::Write, M: ValueMerger<SST::Value>>(
    readers: Vec<Reader<R, SST::Reader>>,
    mut writer: Writer<W, SST::Writer>,
    mut merger: M) -> io::Result<()> {
    let mut heap: BinaryHeap<HeapItem<Reader<R, SST::Reader>>> = readers.into_iter().map(HeapItem).collect();
    loop {
        let len = heap.len();
        let mut value_merger;
        if let Some(mut head) = heap.peek_mut() {
            writer.write_key(head.0.key())?;
            value_merger = merger.new_value(head.0.value());
            if !head.0.advance()? {
                PeekMut::pop(head);
            }
        } else {
            break;
        }
        for _ in 0..len - 1 {
            if let Some(mut head) = heap.peek_mut() {
                if head.0.key() == writer.current_key() {
                    value_merger.add(head.0.value());
                    if !head.0.advance()? {
                        PeekMut::pop(head) ;
                    }
                    continue;
                }
            }
            break;
        }
        let value = value_merger.flush();
        writer.write_value(&value)?;
    }
    writer.finalize()?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use VoidSSTable;
    use SSTable;
    use super::VoidMerge;
    use std::str;
    use std::collections::BTreeSet;

    fn write_sstable(keys: &[&'static str]) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];
        {
            let mut sstable_writer = VoidSSTable::writer(&mut buffer);
            for &key in keys {
                assert!(sstable_writer.write(key.as_bytes(), &()).is_ok());
            }
            assert!(sstable_writer.finalize().is_ok());
        }
        buffer
    }

    fn merge_test_aux(arrs: &[&[&'static str]]) {
        let sstables = arrs.iter()
            .cloned()
            .map(write_sstable)
            .collect::<Vec<_>>();
        let sstables_ref: Vec<&[u8]> = sstables.iter()
            .map(|s| s.as_ref())
            .collect();
        let mut merged = BTreeSet::new();
        for &arr in arrs.iter() {
            for &s in arr {
                merged.insert(s.to_string());
            }
        }
        let mut w = Vec::new();
        assert!(VoidSSTable::merge(sstables_ref, &mut w, VoidMerge).is_ok());
        let mut reader = VoidSSTable::reader(&w[..]);
        for s in merged {
            assert!(reader.advance().unwrap());
            assert_eq!(s.as_bytes(), reader.key());
        }
        assert!(!reader.advance().unwrap());
    }

    #[test]
    fn test_merge() {
        merge_test_aux(&[]);
        merge_test_aux(&[&["a"]]);
        merge_test_aux(&[&["a","b"], &["ab"]]);
        merge_test_aux(&[&["a","b"], &["a", "b"]]);
        merge_test_aux(&[
                &["happy", "hello",  "payer", "tax"],
                &["habitat", "hello", "zoo"],
                &[],
                &["a"],
            ]);
        merge_test_aux(&[&["a"]]);
        merge_test_aux(&[&["a","b"], &["ab"]]);
        merge_test_aux(&[&["a","b"], &["a", "b"]]);
    }
}
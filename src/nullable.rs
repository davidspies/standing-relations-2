use std::collections::{BTreeMap, HashMap};

pub trait Nullable: Default {
    fn is_empty(&self) -> bool;
}

impl Nullable for isize {
    fn is_empty(&self) -> bool {
        *self == 0
    }
}

impl<K, V> Nullable for HashMap<K, V> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<K, V> Nullable for BTreeMap<K, V> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

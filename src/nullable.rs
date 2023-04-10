use std::collections::{BTreeMap, HashMap};

use generic_map::{clear::Clear, hashed_heap::HashedHeap, GenericMap, RolloverMap};

pub trait Nullable: Default {
    fn is_empty(&self) -> bool;
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

impl<K, V: Clear, const N: usize, M: GenericMap> Nullable for RolloverMap<K, V, N, M>
where
    [V; N]: Default,
{
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<K, V, C: Default> Nullable for HashedHeap<K, V, C> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

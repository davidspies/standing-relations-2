use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
    iter,
};

use derivative::Derivative;

use crate::{
    add_to_value::{AddToValue, ValueChanges},
    is_map::IsMap,
    nullable::Nullable,
};

use self::indexed_heap::{Comparator, IndexedHeap, Max, Min};

mod indexed_heap;

#[derive(Derivative)]
#[derivative(Default(bound = "C: Default"))]
pub struct HashHeap<K, V, C> {
    heap: IndexedHeap<K, C>,
    map: HashMap<K, Entry<V>>,
    changed_indices_scratch: Vec<(K, usize)>,
}

pub type HashMaxHeap<K, V> = HashHeap<K, V, Max>;

pub type HashMinHeap<K, V> = HashHeap<K, V, Min>;

pub struct Entry<V> {
    value: V,
    heap_index: usize,
}

impl<K: Eq + Hash, V, C> HashHeap<K, V, C> {
    pub fn favored_key_value(&self) -> Option<(&K, &V)> {
        let key = self.heap.peek()?;
        Some((key, &self.map.get(key).unwrap().value))
    }
}

impl<K: Eq + Hash, V> HashMaxHeap<K, V> {
    pub fn max_key_value(&self) -> Option<(&K, &V)> {
        self.favored_key_value()
    }
}

impl<K: Eq + Hash, V> HashMinHeap<K, V> {
    pub fn min_key_value(&self) -> Option<(&K, &V)> {
        self.favored_key_value()
    }
}

impl<K, V, C: Default> Nullable for HashHeap<K, V, C> {
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

pub type DrainIter<'a, K, V> =
    iter::Map<hash_map::Drain<'a, K, Entry<V>>, fn((K, Entry<V>)) -> (K, V)>;

impl<K: Clone + Ord + Hash, V, C: Default + Comparator> IsMap<K, V> for HashHeap<K, V, C> {
    type DrainIter<'a> = DrainIter<'a, K, V> where Self: 'a;

    fn len(&self) -> usize {
        self.map.len()
    }

    fn drain(&mut self) -> Self::DrainIter<'_> {
        self.heap.clear();
        self.map.drain().map(|(key, entry)| (key, entry.value))
    }

    fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key).map(|entry| &entry.value)
    }

    fn insert_new(&mut self, key: K, value: V) {
        let new_index = self
            .heap
            .insert(key.clone(), &mut self.changed_indices_scratch);
        let entry = Entry {
            value,
            heap_index: new_index,
        };
        let replaced = self.map.insert(key, entry);
        assert!(replaced.is_none());
        for (k, i) in self.changed_indices_scratch.drain(..) {
            self.map.get_mut(&k).unwrap().heap_index = i;
        }
    }

    fn add<Val: AddToValue<V>>(&mut self, key: K, value: Val) -> ValueChanges
    where
        V: Nullable,
    {
        let result = match self.map.entry(key) {
            hash_map::Entry::Occupied(mut occ) => {
                let result = value.add_to(&mut occ.get_mut().value);
                if occ.get().value.is_empty() {
                    let entry = occ.remove();
                    self.heap
                        .remove(entry.heap_index, &mut self.changed_indices_scratch);
                }
                result
            }
            hash_map::Entry::Vacant(vac) => {
                let new_index = self
                    .heap
                    .insert(vac.key().clone(), &mut self.changed_indices_scratch);
                let mut v = V::default();
                let result = value.add_to(&mut v);
                assert!(!v.is_empty());
                let entry = Entry {
                    value: v,
                    heap_index: new_index,
                };
                vac.insert(entry);
                result
            }
        };
        for (k, i) in self.changed_indices_scratch.drain(..) {
            self.map.get_mut(&k).unwrap().heap_index = i;
        }
        result
    }
}

use std::collections::{btree_map, hash_map, BTreeMap, HashMap};
use std::hash::Hash;
use std::mem;

use crate::nullable::Nullable;

pub trait IsMap<K, V>: Nullable {
    type DrainIter<'a>: Iterator<Item = (K, V)> + 'a
    where
        Self: 'a;

    fn len(&self) -> usize;
    fn drain(&mut self) -> Self::DrainIter<'_>;
    fn contains_key(&self, key: &K) -> bool;
    fn get(&self, key: &K) -> Option<&V>;
    fn from_singleton(key: K, value: V) -> Self;
    fn entry_or_default(&mut self, key: K) -> &mut V
    where
        V: Default;
    fn on_entry_then_remove_null_or_on_insert_default<R>(
        &mut self,
        key: K,
        f: impl FnOnce(&mut V) -> R,
    ) -> R
    where
        V: Nullable;
}

impl<K: Eq + Hash, V> IsMap<K, V> for HashMap<K, V> {
    type DrainIter<'a> = hash_map::Drain<'a, K, V> where Self: 'a;

    fn len(&self) -> usize {
        self.len()
    }
    fn drain(&mut self) -> Self::DrainIter<'_> {
        self.drain()
    }
    fn contains_key(&self, key: &K) -> bool {
        self.contains_key(key)
    }
    fn get(&self, key: &K) -> Option<&V> {
        self.get(key)
    }
    fn from_singleton(key: K, value: V) -> Self {
        Self::from_iter([(key, value)])
    }
    fn entry_or_default(&mut self, key: K) -> &mut V
    where
        V: Default,
    {
        self.entry(key).or_default()
    }
    fn on_entry_then_remove_null_or_on_insert_default<R>(
        &mut self,
        key: K,
        f: impl FnOnce(&mut V) -> R,
    ) -> R
    where
        V: Nullable,
    {
        match self.entry(key) {
            hash_map::Entry::Occupied(mut occ) => {
                let result = f(occ.get_mut());
                if occ.get().is_empty() {
                    occ.remove();
                }
                result
            }
            hash_map::Entry::Vacant(vac) => f(vac.insert(V::default())),
        }
    }
}

impl<K: Ord, V> IsMap<K, V> for BTreeMap<K, V> {
    type DrainIter<'a> = btree_map::IntoIter<K,V> where Self: 'a;

    fn len(&self) -> usize {
        self.len()
    }
    fn drain(&mut self) -> Self::DrainIter<'_> {
        mem::take(self).into_iter()
    }
    fn contains_key(&self, key: &K) -> bool {
        self.contains_key(key)
    }
    fn get(&self, key: &K) -> Option<&V> {
        self.get(key)
    }
    fn from_singleton(key: K, value: V) -> Self {
        Self::from_iter([(key, value)])
    }
    fn entry_or_default(&mut self, key: K) -> &mut V
    where
        V: Default,
    {
        self.entry(key).or_default()
    }
    fn on_entry_then_remove_null_or_on_insert_default<R>(
        &mut self,
        key: K,
        f: impl FnOnce(&mut V) -> R,
    ) -> R
    where
        V: Nullable,
    {
        match self.entry(key) {
            btree_map::Entry::Occupied(mut occ) => {
                let result = f(occ.get_mut());
                if occ.get().is_empty() {
                    occ.remove();
                }
                result
            }
            btree_map::Entry::Vacant(vac) => f(vac.insert(V::default())),
        }
    }
}

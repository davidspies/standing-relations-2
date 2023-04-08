use std::{
    array,
    collections::HashMap,
    hash::Hash,
    iter::{Chain, Zip},
    mem, slice,
};

use arrayvec::ArrayVec;
use derivative::Derivative;

use crate::{
    add_to_value::{AddToValue, ValueChanges},
    hash_heap::{HashMaxHeap, HashMinHeap},
    is_map::IsMap,
    nullable::Nullable,
};

#[derive(Debug, Derivative)]
#[derivative(Default(bound = "[V; N]: Default, M: Default"))]
pub struct RolloverMap<K, V, const N: usize = 1, M = HashMap<K, V>> {
    stack_keys: ArrayVec<K, N>,
    stack_values: [V; N],
    heap: M,
}

pub type Iter<'a, K, V, M = HashMap<K, V>> =
    Chain<Zip<slice::Iter<'a, K>, slice::Iter<'a, V>>, <&'a M as IntoIterator>::IntoIter>;

pub type IntoIter<K, V, const N: usize, M = HashMap<K, V>> =
    Chain<Zip<arrayvec::IntoIter<K, N>, array::IntoIter<V, N>>, <M as IntoIterator>::IntoIter>;

impl<K, V, const N: usize, M> RolloverMap<K, V, N, M> {
    pub fn new() -> Self
    where
        [V; N]: Default,
        M: Default,
    {
        Self::default()
    }

    pub fn is_empty(&self) -> bool
    where
        M: Nullable,
    {
        self.stack_keys.is_empty() && self.heap.is_empty()
    }

    pub fn iter<'a>(&'a self) -> Iter<'a, K, V, M>
    where
        &'a M: IntoIterator<Item = (&'a K, &'a V)>,
    {
        self.stack_keys
            .iter()
            .zip(self.stack_values.iter())
            .chain(self.heap.into_iter())
    }

    pub fn into_iter(self) -> IntoIter<K, V, N, M>
    where
        M: IntoIterator<Item = (K, V)>,
    {
        self.stack_keys
            .into_iter()
            .zip(self.stack_values.into_iter())
            .chain(self.heap.into_iter())
    }

    pub(crate) fn into_singleton(self) -> Option<(K, V)> {
        (self.stack_keys.len() == 1).then(|| {
            (
                self.stack_keys.into_iter().next().unwrap(),
                self.stack_values.into_iter().next().unwrap(),
            )
        })
    }

    pub fn get_singleton(&self) -> Option<(&K, &V)> {
        (self.stack_keys.len() == 1).then(|| (&self.stack_keys[0], &self.stack_values[0]))
    }
}

impl<K: Eq, V, const N: usize, M: IsMap<K, V>> RolloverMap<K, V, N, M> {
    pub(crate) fn drain(&mut self) -> impl Iterator<Item = (K, V)> + '_
    where
        V: Default,
    {
        self.stack_keys
            .drain(..)
            .zip(self.stack_values.iter_mut().map(mem::take))
            .chain(self.heap.drain())
    }

    pub fn contains_key(&self, key: &K) -> bool {
        if self.stack_keys.is_empty() {
            self.heap.contains_key(key)
        } else {
            self.stack_keys.contains(key)
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if self.stack_keys.is_empty() {
            self.heap.get(key)
        } else {
            self.stack_keys
                .iter()
                .zip(self.stack_values.iter())
                .find_map(|(k, v)| (k == key).then(|| v))
        }
    }

    pub(crate) fn add(&mut self, key: K, value: impl AddToValue<V>) -> ValueChanges
    where
        V: Nullable,
    {
        let mut iter = self.stack_keys.iter_mut().zip(self.stack_values.iter_mut());
        for (mut k, mut v) in &mut iter {
            if k == &key {
                let result = value.add_to(v);
                if v.is_empty() {
                    for (next_k, next_v) in iter {
                        mem::swap(k, next_k);
                        mem::swap(v, next_v);
                        k = next_k;
                        v = next_v;
                    }
                    self.stack_keys.pop();
                }
                return result;
            }
        }

        if self.stack_keys.len() == N {
            for (k, v) in self
                .stack_keys
                .drain(..)
                .zip(self.stack_values.iter_mut().map(mem::take))
            {
                self.heap.insert_new(k, v);
            }
        }

        if self.heap.is_empty() {
            self.stack_keys.push(key);
            value.add_to(&mut self.stack_values[self.stack_keys.len() - 1])
        } else {
            let result = self.heap.add(key, value);
            if self.heap.len() == N {
                for ((k, v), val) in self.heap.drain().zip(self.stack_values.iter_mut()) {
                    self.stack_keys.push(k);
                    *val = v;
                }
            }
            result
        }
    }
}

pub type RolloverHashMaxHeap<K, V, const N: usize = 1> = RolloverMap<K, V, N, HashMaxHeap<K, V>>;

pub type RolloverHashMinHeap<K, V, const N: usize = 1> = RolloverMap<K, V, N, HashMinHeap<K, V>>;

impl<K: Ord + Hash, V, const N: usize> RolloverHashMaxHeap<K, V, N> {
    pub fn max_key_value(&self) -> Option<(&K, &V)> {
        if self.stack_keys.is_empty() {
            self.heap.max_key_value()
        } else {
            self.stack_keys
                .iter()
                .zip(&self.stack_values)
                .max_by(|(k1, _), (k2, _)| k1.cmp(k2))
        }
    }
}

impl<K: Ord + Hash, V, const N: usize> RolloverHashMinHeap<K, V, N> {
    pub fn min_key_value(&self) -> Option<(&K, &V)> {
        if self.stack_keys.is_empty() {
            self.heap.min_key_value()
        } else {
            self.stack_keys
                .iter()
                .zip(&self.stack_values)
                .min_by(|(k1, _), (k2, _)| k1.cmp(k2))
        }
    }
}

impl<'a, K, V, const N: usize, M> IntoIterator for &'a RolloverMap<K, V, N, M>
where
    &'a M: IntoIterator<Item = (&'a K, &'a V)>,
{
    type Item = (&'a K, &'a V);

    type IntoIter = Iter<'a, K, V, M>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<K, V, const N: usize, M> IntoIterator for RolloverMap<K, V, N, M>
where
    M: IntoIterator<Item = (K, V)>,
{
    type Item = (K, V);

    type IntoIter = IntoIter<K, V, N, M>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_iter()
    }
}

impl<K, V: Default, M: Nullable, const N: usize> Nullable for RolloverMap<K, V, N, M>
where
    [V; N]: Default,
{
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<T: AddToValue<V>, K: Eq + Hash, V: Nullable, const N: usize, M: IsMap<K, V>>
    AddToValue<RolloverMap<K, V, N, M>> for (K, T)
{
    fn add_to(self, v: &mut RolloverMap<K, V, N, M>) -> ValueChanges {
        let (key, value) = self;
        v.add(key, value)
    }
}

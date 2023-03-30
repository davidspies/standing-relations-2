use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
    iter::Chain,
    mem, option,
};

use derivative::Derivative;

use crate::{
    add_to_value::{AddToValue, ValueChanges},
    is_map::IsMap,
    nullable::Nullable,
};

#[derive(Debug, Derivative)]
#[derivative(Default(bound = "V: Default, M: Default"))]
pub struct E1Map<K, V, M = HashMap<K, V>> {
    singleton_key: Option<K>,
    singleton_value: V,
    non_singleton: M,
}

pub type Iter<'a, K, V, M = HashMap<K, V>> =
    Chain<option::IntoIter<(&'a K, &'a V)>, <&'a M as IntoIterator>::IntoIter>;

pub type IntoIter<K, V, M = HashMap<K, V>> =
    Chain<option::IntoIter<(K, V)>, <M as IntoIterator>::IntoIter>;

impl<K, V, M> E1Map<K, V, M> {
    pub fn new() -> Self
    where
        V: Default,
        M: Default,
    {
        Self::default()
    }

    pub fn is_empty(&self) -> bool
    where
        M: Nullable,
    {
        self.singleton_key.is_none() && self.non_singleton.is_empty()
    }

    pub fn iter<'a>(&'a self) -> Iter<'a, K, V, M>
    where
        &'a M: IntoIterator<Item = (&'a K, &'a V)>,
    {
        self.get_singleton()
            .into_iter()
            .chain(self.non_singleton.into_iter())
    }

    pub fn into_iter(self) -> IntoIter<K, V, M>
    where
        M: IntoIterator<Item = (K, V)>,
    {
        self.singleton_key
            .map(|k| (k, self.singleton_value))
            .into_iter()
            .chain(self.non_singleton.into_iter())
    }

    pub(crate) fn into_singleton(self) -> Option<(K, V)> {
        self.singleton_key.map(|k| (k, self.singleton_value))
    }

    pub fn get_singleton(&self) -> Option<(&K, &V)> {
        self.singleton_key
            .as_ref()
            .map(|k| (k, &self.singleton_value))
    }
}

impl<K: Eq, V, M: IsMap<K, V>> E1Map<K, V, M> {
    pub(crate) fn drain(&mut self) -> impl Iterator<Item = (K, V)> + '_
    where
        V: Default,
    {
        self.singleton_key
            .take()
            .map(|k| (k, mem::take(&mut self.singleton_value)))
            .into_iter()
            .chain(self.non_singleton.drain())
    }

    pub fn contains_key(&self, key: &K) -> bool {
        match &self.singleton_key {
            Some(k) => key == k,
            None => self.non_singleton.contains_key(key),
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        match &self.singleton_key {
            Some(k) => (key == k).then_some(&self.singleton_value),
            None => self.non_singleton.get(key),
        }
    }

    pub(crate) fn add(&mut self, key: K, value: impl AddToValue<V>) -> ValueChanges
    where
        V: Nullable,
    {
        match self.singleton_key.take() {
            Some(k) => {
                if key == k {
                    let result = value.add_to(&mut self.singleton_value);
                    if !self.singleton_value.is_empty() {
                        self.singleton_key = Some(k);
                    }
                    result
                } else {
                    let replaced = self
                        .non_singleton
                        .insert(k, mem::take(&mut self.singleton_value));
                    assert!(replaced.is_none());
                    let result = self.non_singleton.add(key, value);
                    assert_eq!(self.non_singleton.len(), 2);
                    result
                }
            }
            None => {
                if self.non_singleton.is_empty() {
                    self.singleton_key = Some(key);
                    let result = value.add_to(&mut self.singleton_value);
                    assert!(!self.singleton_value.is_empty());
                    result
                } else {
                    let result = self.non_singleton.add(key, value);
                    if self.non_singleton.len() == 1 {
                        let (k, v) = self.non_singleton.drain().next().unwrap();
                        self.singleton_key = Some(k);
                        self.singleton_value = v;
                    }
                    result
                }
            }
        }
    }
}

pub type E1BTreeMap<K, V> = E1Map<K, V, BTreeMap<K, V>>;

impl<K: Ord, V> E1BTreeMap<K, V> {
    pub fn first_key_value(&self) -> Option<(&K, &V)> {
        match &self.singleton_key {
            Some(k) => Some((k, &self.singleton_value)),
            None => self.non_singleton.first_key_value(),
        }
    }

    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        match &self.singleton_key {
            Some(k) => Some((k, &self.singleton_value)),
            None => self.non_singleton.last_key_value(),
        }
    }
}

impl<'a, K, V, M> IntoIterator for &'a E1Map<K, V, M>
where
    &'a M: IntoIterator<Item = (&'a K, &'a V)>,
{
    type Item = (&'a K, &'a V);

    type IntoIter = Iter<'a, K, V, M>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<K, V, M> IntoIterator for E1Map<K, V, M>
where
    M: IntoIterator<Item = (K, V)>,
{
    type Item = (K, V);

    type IntoIter = IntoIter<K, V, M>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_iter()
    }
}

impl<K, V: Default, M: Nullable> Nullable for E1Map<K, V, M> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<T: AddToValue<V>, K: Eq + Hash, V: Nullable, M: IsMap<K, V>> AddToValue<E1Map<K, V, M>>
    for (K, T)
{
    fn add_to(self, v: &mut E1Map<K, V, M>) -> ValueChanges {
        let (key, value) = self;
        v.add(key, value)
    }
}

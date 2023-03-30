use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
    iter::Chain,
    option,
};

use derivative::Derivative;

use crate::{
    add_to_value::{AddToValue, ValueChanges},
    is_map::IsMap,
    nullable::Nullable,
};

#[derive(Debug, Derivative)]
#[derivative(Default(bound = "M: Default"))]
pub struct E1Map<K, V, M = HashMap<K, V>> {
    singleton: Option<(K, V)>,
    non_singleton: M,
}

pub type Iter<'a, K, V, M = HashMap<K, V>> =
    Chain<option::IntoIter<(&'a K, &'a V)>, <&'a M as IntoIterator>::IntoIter>;

pub type IntoIter<K, V, M = HashMap<K, V>> =
    Chain<option::IntoIter<(K, V)>, <M as IntoIterator>::IntoIter>;

impl<K, V, M> E1Map<K, V, M> {
    pub fn new() -> Self
    where
        M: Default,
    {
        Self::default()
    }

    pub fn is_empty(&self) -> bool
    where
        M: Nullable,
    {
        self.singleton.is_none() && self.non_singleton.is_empty()
    }

    pub fn iter<'a>(&'a self) -> Iter<'a, K, V, M>
    where
        &'a M: IntoIterator<Item = (&'a K, &'a V)>,
    {
        self.singleton
            .as_ref()
            .map(|(k, v)| (k, v))
            .into_iter()
            .chain(self.non_singleton.into_iter())
    }

    pub fn into_iter(self) -> IntoIter<K, V, M>
    where
        M: IntoIterator<Item = (K, V)>,
    {
        self.singleton
            .into_iter()
            .chain(self.non_singleton.into_iter())
    }

    pub(crate) fn into_singleton(self) -> Option<(K, V)> {
        self.singleton
    }

    pub fn get_singleton(&self) -> Option<(&K, &V)> {
        self.singleton.as_ref().map(|(k, v)| (k, v))
    }
}

impl<K: Eq, V, M: IsMap<K, V>> E1Map<K, V, M> {
    pub(crate) fn drain(&mut self) -> impl Iterator<Item = (K, V)> + '_ {
        self.singleton
            .take()
            .into_iter()
            .chain(self.non_singleton.drain())
    }

    pub fn contains_key(&self, key: &K) -> bool {
        match &self.singleton {
            Some((k, _)) => key == k,
            None => self.non_singleton.contains_key(key),
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        match &self.singleton {
            Some((k, v)) => (key == k).then_some(v),
            None => self.non_singleton.get(key),
        }
    }

    pub(crate) fn add(&mut self, key: K, value: impl AddToValue<V>) -> ValueChanges
    where
        V: Nullable,
    {
        match self.singleton.take() {
            Some((k, v)) => {
                if key == k {
                    self.singleton = Some((k, v));
                    let v = &mut self.singleton.as_mut().unwrap().1;
                    let result = value.add_to(v);
                    if v.is_empty() {
                        self.singleton = None
                    }
                    result
                } else {
                    self.non_singleton = M::from_singleton(k, v);
                    let v = self.non_singleton.entry_or_default(key);
                    let result = value.add_to(v);
                    if v.is_empty() {
                        panic!("Still empty")
                    }
                    result
                }
            }
            None => {
                if self.non_singleton.is_empty() {
                    self.singleton = Some((key, V::default()));
                    let v = &mut self.singleton.as_mut().unwrap().1;
                    let result = value.add_to(v);
                    if v.is_empty() {
                        panic!("Still empty")
                    }
                    result
                } else {
                    let result = self
                        .non_singleton
                        .on_entry_then_remove_null_or_on_insert_default(key, |v| value.add_to(v));
                    if self.non_singleton.len() == 1 {
                        let (k, v) = self.non_singleton.drain().next().unwrap();
                        self.singleton = Some((k, v));
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
        match &self.singleton {
            Some((k, v)) => Some((k, v)),
            None => self.non_singleton.first_key_value(),
        }
    }

    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        match &self.singleton {
            Some((k, v)) => Some((k, v)),
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

impl<K, V, M: Nullable> Nullable for E1Map<K, V, M> {
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

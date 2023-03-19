use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
    iter::Chain,
    option,
};

use derivative::Derivative;

use crate::value_count::ValueCount;

pub(crate) trait Nullable: Default {
    fn is_empty(&self) -> bool;
}

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
pub(crate) struct E1Map<K, V> {
    singleton: Option<(K, V)>,
    non_singleton: HashMap<K, V>,
}

impl<K, V> E1Map<K, V> {
    pub(crate) fn get(&self, key: &K) -> Option<&V>
    where
        K: Eq + Hash,
    {
        match &self.singleton {
            Some((k, v)) => (key == k).then_some(v),
            None => self.non_singleton.get(key),
        }
    }

    pub(crate) fn add(&mut self, key: K, value: impl AddToValue<V>) -> ValueChanges
    where
        K: Eq + Hash,
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
                    self.non_singleton = HashMap::from_iter([(k, v)]);
                    let v = &mut self.non_singleton.entry(key).or_default();
                    let result = value.add_to(v);
                    if v.is_empty() {
                        panic!("Still empty")
                    }
                    result
                }
            }
            None => {
                if self.non_singleton.capacity() == 0 {
                    self.singleton = Some((key, V::default()));
                    let v = &mut self.singleton.as_mut().unwrap().1;
                    let result = value.add_to(v);
                    if v.is_empty() {
                        panic!("Still empty")
                    }
                    result
                } else {
                    match self.non_singleton.entry(key) {
                        hash_map::Entry::Occupied(mut occ) => {
                            let v = &mut occ.get_mut();
                            let result = value.add_to(v);
                            if v.is_empty() {
                                occ.remove();
                            }
                            result
                        }
                        hash_map::Entry::Vacant(vac) => {
                            let v = vac.insert(V::default());
                            let result = value.add_to(v);
                            if v.is_empty() {
                                panic!("Still empty")
                            }
                            result
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn drain(&mut self) -> impl Iterator<Item = (K, V)> + '_ {
        self.singleton
            .take()
            .into_iter()
            .chain(self.non_singleton.drain())
    }
}

impl<'a, K, V> IntoIterator for &'a E1Map<K, V> {
    type Item = (&'a K, &'a V);

    type IntoIter = Chain<option::IntoIter<(&'a K, &'a V)>, hash_map::Iter<'a, K, V>>;

    fn into_iter(self) -> Self::IntoIter {
        self.singleton
            .as_ref()
            .map(|(k, v)| (k, v))
            .into_iter()
            .chain(self.non_singleton.iter())
    }
}

pub(crate) struct ValueChanges {
    pub(crate) was_zero: bool,
    pub(crate) is_zero: bool,
}

pub(crate) trait AddToValue<V> {
    #[must_use]
    fn add_to(self, v: &mut V) -> ValueChanges;
}

impl Nullable for ValueCount {
    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl AddToValue<ValueCount> for ValueCount {
    fn add_to(self, v: &mut ValueCount) -> ValueChanges {
        let was_zero = v.count == 0;
        *v += self;
        let is_zero = v.count == 0;
        ValueChanges { was_zero, is_zero }
    }
}

impl<K, V> Nullable for E1Map<K, V> {
    fn is_empty(&self) -> bool {
        self.singleton.is_none() && self.non_singleton.is_empty()
    }
}

impl<T: AddToValue<V>, K: Eq + Hash, V: Nullable> AddToValue<E1Map<K, V>> for (K, T) {
    fn add_to(self, v: &mut E1Map<K, V>) -> ValueChanges {
        let (key, value) = self;
        v.add(key, value)
    }
}

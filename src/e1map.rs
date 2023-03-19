use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
};

use derivative::Derivative;

use crate::value_count::ValueCount;

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
pub(crate) struct E1Map<K, V> {
    singleton: Option<(K, V)>,
    non_singleton: HashMap<K, V>,
}

impl<K, V> E1Map<K, V> {
    fn is_empty(&self) -> bool {
        self.singleton.is_none() && self.non_singleton.is_empty()
    }

    pub(crate) fn get(&self, key: &K) -> Option<&V>
    where
        K: Eq + Hash,
    {
        match &self.singleton {
            Some((k, v)) => (key == k).then_some(v),
            None => self.non_singleton.get(key),
        }
    }

    pub(crate) fn add(&mut self, key: K, value: impl AddToValue<V>)
    where
        K: Eq + Hash,
        V: Default,
    {
        match self.singleton.take() {
            Some((k, v)) => {
                if key == k {
                    self.singleton = Some((k, v));
                    if !value.add_to(&mut self.singleton.as_mut().unwrap().1) {
                        self.singleton = None
                    }
                } else {
                    self.non_singleton = HashMap::from_iter([(k, v)]);
                    if !value.add_to(self.non_singleton.entry(key).or_default()) {
                        panic!("Still empty")
                    }
                }
            }
            None => {
                if self.non_singleton.capacity() == 0 {
                    self.singleton = Some((key, V::default()));
                    if !value.add_to(&mut self.singleton.as_mut().unwrap().1) {
                        panic!("Still empty")
                    }
                } else {
                    match self.non_singleton.entry(key) {
                        hash_map::Entry::Occupied(mut occ) => {
                            if !value.add_to(occ.get_mut()) {
                                occ.remove();
                            }
                        }
                        hash_map::Entry::Vacant(vac) => {
                            if !value.add_to(vac.insert(V::default())) {
                                panic!("Still empty")
                            }
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

impl<K, V> E1Map<K, V> {
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.singleton
            .as_ref()
            .map(|(k, v)| (k, v))
            .into_iter()
            .chain(self.non_singleton.iter())
    }
}

pub(crate) trait AddToValue<V> {
    #[must_use]
    fn add_to(self, v: &mut V) -> bool;
}

impl AddToValue<ValueCount> for ValueCount {
    fn add_to(self, v: &mut ValueCount) -> bool {
        *v += self;
        v.count != 0
    }
}

impl<T: AddToValue<V>, K: Eq + Hash, V: Default> AddToValue<E1Map<K, V>> for (K, T) {
    fn add_to(self, v: &mut E1Map<K, V>) -> bool {
        let (key, value) = self;
        v.add(key, value);
        !v.is_empty()
    }
}

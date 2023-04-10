use generic_map::{clear::Clear, GenericMap, OccupiedEntry, VacantEntry};

use crate::{nullable::Nullable, value_count::ValueCount};

pub trait SingletonMap: GenericMap {
    fn get_singleton(&self) -> Option<(&Self::K, &Self::V)> {
        let mut iter = self.iter();
        let (k, v) = iter.next()?;
        if iter.next().is_some() {
            None
        } else {
            Some((k, v))
        }
    }
    fn into_singleton(self) -> Option<(Self::K, Self::V)> {
        let mut iter = self.into_iter();
        let (k, v) = iter.next()?;
        if iter.next().is_some() {
            None
        } else {
            Some((k, v))
        }
    }
}

impl<T: GenericMap> SingletonMap for T {}

pub trait AddMap<V>: Nullable + Clear {
    fn add(&mut self, v: V);
}

impl AddMap<ValueCount> for ValueCount {
    fn add(&mut self, v: ValueCount) {
        *self += v;
    }
}

impl<M1: GenericMap<K = K, V = M2> + Clear + Nullable, M2: AddMap<V>, K, V> AddMap<(K, V)> for M1 {
    fn add(&mut self, (k, v): (K, V)) {
        match self.entry(k) {
            generic_map::Entry::Vacant(vac) => vac.insert(M2::default()).add(v),
            generic_map::Entry::Occupied(mut occ) => {
                occ.get_mut().add(v);
                if occ.get().is_empty() {
                    occ.remove_clearable();
                }
            }
        }
    }
}

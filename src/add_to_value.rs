use std::collections::{btree_map, BTreeMap};

use crate::nullable::Nullable;

pub trait AddToValue<V> {
    #[must_use]
    fn add_to(self, v: &mut V) -> ValueChanges;
}

pub struct ValueChanges {
    pub(crate) was_zero: bool,
    pub(crate) is_zero: bool,
}

impl AddToValue<isize> for isize {
    fn add_to(self, v: &mut isize) -> ValueChanges {
        let was_zero = *v == 0;
        *v += self;
        let is_zero = *v == 0;
        ValueChanges { was_zero, is_zero }
    }
}

impl<T: AddToValue<V>, K: Ord, V: Nullable> AddToValue<BTreeMap<K, V>> for (K, T) {
    fn add_to(self, v: &mut BTreeMap<K, V>) -> ValueChanges {
        let (key, value) = self;
        let entry = v.entry(key);
        match entry {
            btree_map::Entry::Vacant(vac) => value.add_to(vac.insert(V::default())),
            btree_map::Entry::Occupied(mut occ) => {
                let result = value.add_to(occ.get_mut());
                if occ.get().is_empty() {
                    occ.remove();
                }
                result
            }
        }
    }
}

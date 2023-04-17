use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
};

use crate::{
    context::{CommitId, DataId, Ids},
    entry::Entry,
    generic_map::AddMap,
    nullable::Nullable,
    op::Op,
    relation::RelationInner,
    value_count::ValueCount,
};

pub struct Consolidate<T, C> {
    sub_rel: RelationInner<T, C>,
    collected1_scratch: Vec<Entry<T>>,
    within_id_scratch: HashMap<T, ValueCount>,
    collected2_scratch: Vec<Entry<T>>,
    consolidated_scratch: HashMap<(T, DataId), ValueCount>,
    unsent_scratch: HashMap<(T, DataId), ValueCount>,
}

impl<T, C> Consolidate<T, C> {
    pub(crate) fn new(sub_rel: RelationInner<T, C>) -> Self {
        Self {
            sub_rel,
            collected1_scratch: Vec::new(),
            within_id_scratch: HashMap::new(),
            collected2_scratch: Vec::new(),
            consolidated_scratch: HashMap::new(),
            unsent_scratch: HashMap::new(),
        }
    }
}

impl<T: Clone + Eq + Hash, C: Op<T>> Op<T> for Consolidate<T, C> {
    fn type_name(&self) -> &'static str {
        "consolidate"
    }
    fn foreach<F: FnMut(T, Ids, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.sub_rel
            .dump_to_vec(current_id, &mut self.collected1_scratch);
        let mut iter = self.collected1_scratch.drain(..).peekable();
        while let Some(e) = iter.peek() {
            let ids = e.ids;
            while iter.peek().map(|e| e.ids) == Some(ids) {
                let Entry {
                    value, value_count, ..
                } = iter.next().unwrap();
                self.within_id_scratch.add((value, value_count));
            }
            for (value, value_count) in self.within_id_scratch.drain() {
                self.consolidated_scratch
                    .add(((value.clone(), ids.data_id()), value_count));
                self.collected2_scratch.push(Entry {
                    value,
                    ids,
                    value_count,
                });
            }
        }
        for entry in self.collected2_scratch.drain(..) {
            let k = (entry.value.clone(), entry.ids.data_id());
            self.unsent_scratch.add((k.clone(), entry.value_count));
            let hash_map::Entry::Occupied(mut occ1) = self.unsent_scratch.entry(k.clone())
            else {
                continue;
            };
            let hash_map::Entry::Occupied(mut occ2) = self.consolidated_scratch.entry(k)
            else {
                continue;
            };
            let v1 = occ1.get_mut();
            let v2 = occ2.get_mut();
            if v1.signum() != v2.signum() {
                continue;
            }
            let vmin = v1.min_magnitude(*v2);
            f(entry.value, entry.ids, vmin);
            *v1 -= vmin;
            *v2 -= vmin;
            if v1.is_empty() {
                occ1.remove();
            }
            if v2.is_empty() {
                occ2.remove();
            }
        }
        assert!(self.consolidated_scratch.is_empty());
        assert!(self.unsent_scratch.is_empty());
    }
}

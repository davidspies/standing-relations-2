use std::hash::Hash;

use crate::{e1map::E1Map, op::Op, relation::Relation, value_count::ValueCount};

pub struct Consolidate<T, C> {
    sub_rel: Relation<T, C>,
    scratch_space: E1Map<T, ValueCount>,
}

impl<T, C> Consolidate<T, C> {
    pub fn new(sub_rel: Relation<T, C>) -> Self {
        Self {
            sub_rel,
            scratch_space: E1Map::default(),
        }
    }
}

impl<T: Eq + Hash, C: Op<T>> Op<T> for Consolidate<T, C> {
    fn foreach(&mut self, mut f: impl FnMut(T, ValueCount)) {
        self.sub_rel.foreach(|value, count| {
            self.scratch_space.add(value, count);
        });
        self.scratch_space
            .drain()
            .for_each(|(value, count)| f(value, count));
    }
}

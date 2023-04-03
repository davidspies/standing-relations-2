use std::hash::Hash;

use crate::{
    context::CommitId, op::Op, relation::RelationInner, rollover_map::RolloverMap,
    value_count::ValueCount,
};

pub struct Consolidate<T, C> {
    sub_rel: RelationInner<T, C>,
    collected_scratch: RolloverMap<T, ValueCount>,
}

impl<T, C> Consolidate<T, C> {
    pub(crate) fn new(sub_rel: RelationInner<T, C>) -> Self {
        Self {
            sub_rel,
            collected_scratch: RolloverMap::default(),
        }
    }
}

impl<T: Eq + Hash, C: Op<T>> Op<T> for Consolidate<T, C> {
    fn type_name(&self) -> &'static str {
        "consolidate"
    }
    fn foreach<F: FnMut(T, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.sub_rel
            .dump_to_map(current_id, &mut self.collected_scratch);
        self.collected_scratch
            .drain()
            .for_each(|(value, count)| f(value, count));
    }
}

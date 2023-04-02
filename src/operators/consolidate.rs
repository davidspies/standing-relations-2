use std::{collections::HashMap, hash::Hash};

use crate::{context::CommitId, op::Op, relation::RelationInner, value_count::ValueCount};

pub struct Consolidate<T, C> {
    sub_rel: RelationInner<T, C>,
    consolidated_scratch: HashMap<T, ValueCount>,
}

impl<T, C> Consolidate<T, C> {
    pub(crate) fn new(sub_rel: RelationInner<T, C>) -> Self {
        Self {
            sub_rel,
            consolidated_scratch: HashMap::new(),
        }
    }
}

impl<T: Clone + Eq + Hash, C: Op<T>> Op<T> for Consolidate<T, C> {
    fn type_name(&self) -> &'static str {
        "consolidate"
    }
    fn foreach<F: FnMut(T, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.sub_rel
            .dump_to_map(current_id, &mut self.consolidated_scratch);
        for (value, value_count) in self.consolidated_scratch.drain() {
            f(value, value_count);
        }
    }
}

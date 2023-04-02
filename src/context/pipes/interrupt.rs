use std::hash::Hash;

use crate::{
    context::{CommitId, Dropped},
    e1map::E1Map,
    op::Op,
    relation::RelationInner,
    value_count::ValueCount,
};

use super::{PipeT, ProcessResult};

pub type InterruptId = usize;

pub struct Interrupt<T, C> {
    relation: RelationInner<T, C>,
    interrupt_id: InterruptId,
    values: E1Map<T, ValueCount>,
}

impl<T, C> Interrupt<T, C> {
    pub(crate) fn new(interrupt_id: InterruptId, relation: RelationInner<T, C>) -> Self {
        Self {
            relation,
            interrupt_id,
            values: E1Map::new(),
        }
    }
}

impl<T: Eq + Hash, C: Op<T>> PipeT for Interrupt<T, C> {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        self.relation.dump_to_map(commit_id, &mut self.values);
        if self.values.is_empty() {
            Ok(ProcessResult::Unchanged)
        } else {
            Ok(ProcessResult::Interrupted(self.interrupt_id))
        }
    }
    fn push_frame(&mut self) {}
    fn pop_frame(&mut self) -> Result<(), Dropped> {
        Ok(())
    }
}

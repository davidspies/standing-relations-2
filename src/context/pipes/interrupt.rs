use std::hash::Hash;

use generic_map::rollover_map::RolloverMap;

use crate::{
    context::{CommitId, Dropped},
    op::Op,
    relation::RelationInner,
    value_count::ValueCount,
};

use super::{PipeT, ProcessResult};

pub type InterruptId = usize;

pub struct Interrupt<T, C> {
    relation: RelationInner<T, C>,
    interrupt_id: InterruptId,
    values: RolloverMap<T, ValueCount>,
}

impl<T, C> Interrupt<T, C> {
    pub(crate) fn new(interrupt_id: InterruptId, relation: RelationInner<T, C>) -> Self {
        Self {
            relation,
            interrupt_id,
            values: RolloverMap::new(),
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

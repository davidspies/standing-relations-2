use std::hash::Hash;

use crate::{
    context::{CommitId, Dropped},
    relation::Relation,
    E1Map, Op, ValueCount,
};

use super::{PipeT, ProcessResult};

pub type InterruptId = usize;

pub struct Interrupt<T, C> {
    relation: Relation<T, C>,
    interrupt_id: InterruptId,
    values: E1Map<T, ValueCount>,
}

impl<T, C> Interrupt<T, C> {
    pub(crate) fn new(interrupt_id: InterruptId, relation: Relation<T, C>) -> Self {
        Self {
            relation,
            interrupt_id,
            values: E1Map::new(),
        }
    }
}

impl<T: Eq + Hash, C: Op<T>> PipeT for Interrupt<T, C> {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        self.relation.foreach(commit_id, |elem, value_count| {
            self.values.add(elem, value_count);
        });
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

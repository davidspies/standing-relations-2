use std::{collections::HashMap, hash::Hash};

use crate::{
    context::{CommitId, Dropped, Level},
    op::Op,
    relation::RelationInner,
    value_count::ValueCount,
};

use super::{ProcessResult, Processable};

pub type InterruptId = usize;

pub struct Interrupt<T, C> {
    relation: RelationInner<T, C>,
    interrupt_id: InterruptId,
    values: HashMap<(T, Level), ValueCount>,
}

impl<T, C> Interrupt<T, C> {
    pub(crate) fn new(interrupt_id: InterruptId, relation: RelationInner<T, C>) -> Self {
        Self {
            relation,
            interrupt_id,
            values: HashMap::new(),
        }
    }
}

impl<T: Eq + Hash, C: Op<T>> Processable for Interrupt<T, C> {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        self.relation.dump_to_map(commit_id, &mut self.values);
        if self.values.is_empty() {
            Ok(ProcessResult::Unchanged)
        } else {
            Ok(ProcessResult::Interrupted(self.interrupt_id))
        }
    }
}

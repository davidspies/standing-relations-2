use std::{collections::HashSet, hash::Hash};

use crate::{
    context::{CommitId, Dropped},
    op::Op,
    operators::input::Input,
    relation::RelationInner,
};

use super::{PipeT, ProcessResult};

pub(crate) struct FeedbackPipe<T, C> {
    relation: RelationInner<T, C>,
    seen: HashSet<T>,
    frame_changes: Vec<HashSet<T>>,
    input: Input<T>,
}

impl<T, C> FeedbackPipe<T, C> {
    pub(crate) fn new(relation: RelationInner<T, C>, input: Input<T>) -> Self {
        FeedbackPipe {
            relation,
            seen: HashSet::new(),
            frame_changes: Vec::new(),
            input,
        }
    }
}

impl<T: Eq + Hash + Clone, C: Op<T>> PipeT for FeedbackPipe<T, C> {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        let mut any_dropped = false;
        let mut result = ProcessResult::Unchanged;
        self.relation.foreach(commit_id, |elem, _| {
            if self.seen.insert(elem.clone()) {
                result = ProcessResult::Changed;
                if let Some(frame) = self.frame_changes.last_mut() {
                    frame.insert(elem.clone());
                }
                if self.input.send(elem).is_err() {
                    any_dropped = true;
                }
            }
        });
        if any_dropped {
            Err(Dropped)
        } else {
            Ok(result)
        }
    }
    fn push_frame(&mut self) {
        self.frame_changes.push(HashSet::new());
    }
    fn pop_frame(&mut self) -> Result<(), Dropped> {
        let frame = self.frame_changes.pop().unwrap();
        for elem in frame {
            self.seen.remove(&elem);
        }
        Ok(())
    }
}

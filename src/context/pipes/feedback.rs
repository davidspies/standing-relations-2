use std::{collections::HashSet, hash::Hash};

use crate::{
    context::{CommitId, Dropped},
    op::Op,
    operators::input::Input,
    relation::Relation,
};

use super::PipeT;

pub(crate) struct FeedbackPipe<T, C> {
    relation: Relation<T, C>,
    seen: HashSet<T>,
    frame_changes: Vec<HashSet<T>>,
    input: Input<T>,
}

impl<T, C> FeedbackPipe<T, C> {
    pub(crate) fn new(relation: Relation<T, C>, input: Input<T>) -> Self {
        FeedbackPipe {
            relation,
            seen: HashSet::new(),
            frame_changes: Vec::new(),
            input,
        }
    }
}

impl<T: Eq + Hash + Clone, C: Op<T>> PipeT for FeedbackPipe<T, C> {
    fn process(&mut self, commit_id: CommitId) -> Result<bool, Dropped> {
        let mut any_dropped = false;
        let mut any_added = false;
        self.relation.foreach(commit_id, |elem, _| {
            if self.seen.insert(elem.clone()) {
                any_added = true;
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
            Ok(any_added)
        }
    }
    fn push_frame(&mut self) {
        self.frame_changes.push(HashSet::new());
    }
    fn pop_frame(&mut self) -> Result<(), Dropped> {
        let frame = self.frame_changes.pop().unwrap();
        for elem in frame {
            self.seen.remove(&elem);
            if self.input.unsend(elem).is_err() {
                return Err(Dropped);
            }
        }
        Ok(())
    }
}

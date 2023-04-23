use crate::{
    context::{CommitId, Dropped},
    op::Op,
    operators::input::Input,
    relation::RelationInner,
    who::Who,
};

use super::{ProcessResult, Processable};

pub(crate) struct FeedbackPipe<T, C> {
    relation: RelationInner<T, C>,
    input: Input<T>,
}

impl<T, C> FeedbackPipe<T, C> {
    pub(crate) fn new(relation: RelationInner<T, C>, input: Input<T>) -> Self {
        FeedbackPipe { relation, input }
    }
}

impl<T, C: Op<T>> Processable for FeedbackPipe<T, C> {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        let mut any_dropped = false;
        let mut result = ProcessResult::Unchanged;
        self.relation.foreach(commit_id, |value, count| {
            if self.input.send_count(value, Who::Feedback(count)).is_err() {
                any_dropped = true;
            }
            result = ProcessResult::Changed;
        });
        if any_dropped {
            Err(Dropped)
        } else {
            Ok(result)
        }
    }
}

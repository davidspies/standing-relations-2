use derivative::Derivative;

use crate::{
    channel::{Receiver, Sender},
    context::{CommitId, ContextId},
    op::Op,
    relation::Relation,
    value_count::ValueCount,
    who::Who,
};

#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct Input<T> {
    pub(crate) context_id: ContextId,
    sender: Sender<(T, Who)>,
}

pub type InputRelation<T> = Relation<T, InputOp<T>>;

impl<T> Input<T> {
    pub(crate) fn new(context_id: ContextId, sender: Sender<(T, Who)>) -> Self {
        Self { context_id, sender }
    }

    pub(crate) fn send_count(&mut self, elem: T, who: Who) -> Result<(), T> {
        self.sender.send((elem, who)).map_err(|(err, _)| err)
    }

    pub fn send(&mut self, elem: T) -> Result<(), T> {
        self.send_count(elem, Who::User)
    }
}

pub struct InputOp<T>(Receiver<(T, ValueCount)>);

impl<T> InputOp<T> {
    pub(crate) fn new(receiver: Receiver<(T, ValueCount)>) -> Self {
        Self(receiver)
    }
}

impl<T> Op<T> for InputOp<T> {
    fn type_name(&self) -> &'static str {
        "input"
    }
    fn foreach<F: FnMut(T, ValueCount)>(&mut self, _current_id: CommitId, mut f: F) {
        while let Some((value, count)) = self.0.try_recv() {
            f(value, count)
        }
    }
}

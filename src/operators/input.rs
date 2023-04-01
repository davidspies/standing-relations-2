use derivative::Derivative;

use crate::{
    channel::{Receiver, Sender},
    context::{CommitId, ContextId},
    op::Op,
    value_count::ValueCount,
};

#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct Input<T> {
    pub(crate) context_id: ContextId,
    sender: Sender<(T, isize)>,
}

impl<T> Input<T> {
    pub(crate) fn new(context_id: ContextId, sender: Sender<(T, isize)>) -> Self {
        Self { context_id, sender }
    }

    pub fn send(&mut self, elem: T) -> Result<(), T> {
        self.sender.send((elem, 1)).map_err(|(elem, _)| elem)
    }

    pub fn unsend(&mut self, elem: T) -> Result<(), T> {
        self.sender.send((elem, -1)).map_err(|(elem, _)| elem)
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

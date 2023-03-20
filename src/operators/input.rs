use crate::{
    channel::{Receiver, Sender},
    commit_id::CommitId,
    op::Op,
    value_count::ValueCount,
};

pub struct Input<T>(Sender<(T, ValueCount)>);

impl<T> Input<T> {
    pub(crate) fn new(sender: Sender<(T, ValueCount)>) -> Self {
        Self(sender)
    }
}

pub struct InputOp<T>(Receiver<(T, ValueCount)>);

impl<T> InputOp<T> {
    pub(crate) fn new(receiver: Receiver<(T, ValueCount)>) -> Self {
        Self(receiver)
    }
}

impl<T> Op<T> for InputOp<T> {
    fn foreach(&mut self, _current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        while let Some((value, count)) = self.0.try_recv() {
            f(value, count)
        }
    }
}

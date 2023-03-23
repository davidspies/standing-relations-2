use crate::{
    channel::{Receiver, Sender},
    context::CommitId,
    op::Op,
    value_count::ValueCount,
};

pub struct Input<T>(Sender<(T, isize)>);

impl<T> Input<T> {
    pub(crate) fn new(sender: Sender<(T, isize)>) -> Self {
        Self(sender)
    }

    pub(crate) fn send_with_count(&mut self, elem: T, count: isize) -> Result<(), (T, isize)> {
        self.0.send((elem, count))
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

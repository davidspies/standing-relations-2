use crate::{
    channel::{Receiver, Sender},
    commit_id::CommitId,
    op::Op,
    value_count::ValueCount,
};

pub struct Input<T>(Sender<(T, ValueCount)>);

pub struct InputOp<T>(Receiver<(T, ValueCount)>);

impl<T> Op<T> for InputOp<T> {
    fn foreach(&mut self, _current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        while let Some((value, count)) = self.0.try_recv() {
            f(value, count)
        }
    }
}

use crate::{context::CommitId, value_count::ValueCount};

pub trait Op<T> {
    fn foreach(&mut self, current_id: CommitId, f: impl FnMut(T, ValueCount));
}

impl<T, C: Op<T>> Op<T> for Box<C> {
    fn foreach(&mut self, current_id: CommitId, f: impl FnMut(T, ValueCount)) {
        self.as_mut().foreach(current_id, f)
    }
}

pub trait DynOp<T> {
    fn foreach(&mut self, current_id: CommitId, f: &mut dyn FnMut(T, ValueCount));
}

impl<T, C: Op<T>> DynOp<T> for C {
    fn foreach(&mut self, current_id: CommitId, f: &mut dyn FnMut(T, ValueCount)) {
        Op::foreach(self, current_id, f)
    }
}

impl<T> Op<T> for dyn DynOp<T> + '_ {
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        DynOp::foreach(self, current_id, &mut f)
    }
}

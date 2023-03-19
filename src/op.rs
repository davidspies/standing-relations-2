use crate::value_count::ValueCount;

pub trait Op<T> {
    fn foreach(&mut self, f: impl FnMut(T, ValueCount));
}

impl<T, C: Op<T>> Op<T> for Box<C> {
    fn foreach(&mut self, f: impl FnMut(T, ValueCount)) {
        self.as_mut().foreach(f)
    }
}

pub trait DynOp<T> {
    fn foreach(&mut self, f: &mut dyn FnMut(T, ValueCount));
}

impl<T, C: Op<T>> DynOp<T> for C {
    fn foreach(&mut self, f: &mut dyn FnMut(T, ValueCount)) {
        Op::foreach(self, f)
    }
}

impl<T> Op<T> for dyn DynOp<T> + '_ {
    fn foreach(&mut self, mut f: impl FnMut(T, ValueCount)) {
        DynOp::foreach(self, &mut f)
    }
}

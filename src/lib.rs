use e1map::E1Map;
use op::Op;
use value_count::ValueCount;

mod commit_id;
mod e1map;
mod op;
mod operators;
mod relation;
mod value_count;

pub struct InputOp<T> {
    unprocessed_values: E1Map<T, ValueCount>,
}

impl<T> Op<T> for InputOp<T> {
    fn foreach(&mut self, mut f: impl FnMut(T, ValueCount)) {
        self.unprocessed_values
            .drain()
            .for_each(|(value, count)| f(value, count))
    }
}

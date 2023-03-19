use crossbeam_channel::Receiver;

use self::op::Op;
use self::value_count::ValueCount;

pub use relation::Relation;

mod add_to_value;
mod commit_id;
mod e1map;
mod nullable;
mod op;
mod operators;
mod relation;
mod value_count;

pub struct InputOp<T>(Receiver<(T, ValueCount)>);

impl<T> Op<T> for InputOp<T> {
    fn foreach(&mut self, mut f: impl FnMut(T, ValueCount)) {
        while let Ok((value, count)) = self.0.try_recv() {
            f(value, count)
        }
    }
}

use commit_id::CommitId;
use crossbeam_channel::Receiver;

pub use self::e1map::E1Map;
pub use self::op::{DynOp, Op};
pub use self::operators::save::Saved;
pub use self::relation::Relation;
pub use self::value_count::ValueCount;

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
    fn foreach(&mut self, _current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        while let Ok((value, count)) = self.0.try_recv() {
            f(value, count)
        }
    }
}

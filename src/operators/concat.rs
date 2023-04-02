use crate::{context::CommitId, op::Op, relation::RelationInner, value_count::ValueCount};

pub struct Concat<T, CL, CR> {
    left: RelationInner<T, CL>,
    right: RelationInner<T, CR>,
}

impl<T, CL, CR> Concat<T, CL, CR> {
    pub(crate) fn new((left, right): (RelationInner<T, CL>, RelationInner<T, CR>)) -> Self {
        Self { left, right }
    }
}

impl<T, CL: Op<T>, CR: Op<T>> Op<T> for Concat<T, CL, CR> {
    fn type_name(&self) -> &'static str {
        "concat"
    }
    fn foreach<F: FnMut(T, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.left.foreach(current_id, |x, v| f(x, v));
        self.right.foreach(current_id, |x, v| f(x, v));
    }
}

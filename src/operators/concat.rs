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
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        self.left.foreach(current_id, &mut f);
        self.right.foreach(current_id, f);
    }
}

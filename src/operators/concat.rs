use crate::{op::Op, value_count::ValueCount, relation::Relation};

pub struct Concat<T, CL, CR> {
    left: Relation<T, CL>,
    right: Relation<T, CR>,
}

impl<T, CL, CR> Concat<T, CL, CR> {
    pub fn new(left: Relation<T, CL>, right: Relation<T, CR>) -> Self {
        Self { left, right }
    }
}

impl<T, CL: Op<T>, CR: Op<T>> Op<T> for Concat<T, CL, CR> {
    fn foreach(&mut self, mut f: impl FnMut(T, ValueCount)) {
        self.left.foreach(&mut f);
        self.right.foreach(f);
    }
}

use crate::{
    context::{CommitId, Ids},
    entry::Entry,
    op::Op,
    relation::RelationInner,
    value_count::ValueCount,
};

pub struct Concat<T, CL, CR> {
    left: RelationInner<T, CL>,
    right: RelationInner<T, CR>,
    left_scratch: Vec<Entry<T>>,
    right_scratch: Vec<Entry<T>>,
}

impl<T, CL, CR> Concat<T, CL, CR> {
    pub(crate) fn new((left, right): (RelationInner<T, CL>, RelationInner<T, CR>)) -> Self {
        Self {
            left,
            right,
            left_scratch: Vec::new(),
            right_scratch: Vec::new(),
        }
    }
}

impl<T, CL: Op<T>, CR: Op<T>> Op<T> for Concat<T, CL, CR> {
    fn type_name(&self) -> &'static str {
        "concat"
    }
    fn foreach<F: FnMut(T, Ids, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.left.dump_to_vec(current_id, &mut self.left_scratch);
        self.right.dump_to_vec(current_id, &mut self.right_scratch);
        let mut left_iter = self.left_scratch.drain(..).peekable();
        let mut right_iter = self.right_scratch.drain(..).peekable();
        loop {
            match (left_iter.peek(), right_iter.peek()) {
                (Some(left), Some(right)) => {
                    if left.ids <= right.ids {
                        left_iter.next().unwrap().f_on(&mut f);
                    } else {
                        right_iter.next().unwrap().f_on(&mut f);
                    }
                    continue;
                }
                (_, None) => {
                    for entry in left_iter {
                        entry.f_on(&mut f);
                    }
                }
                (None, _) => {
                    for entry in right_iter {
                        entry.f_on(&mut f);
                    }
                }
            }
            break;
        }
    }
}

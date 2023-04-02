use std::{
    cell::{Cell, Ref, RefCell},
    hash::Hash,
    rc::Rc,
};

use crate::{
    context::CommitId,
    e1map::E1Map,
    op::{DynOp, Op},
    operators::save::SavedOp,
    relation::RelationInner,
    value_count::ValueCount,
};

struct OutputInner<T, C> {
    relation: RelationInner<T, C>,
    values: E1Map<T, ValueCount>,
}

impl<T: Eq + Hash, C: Op<T>> OutputInner<T, C> {
    fn update(&mut self, commit_id: CommitId) {
        self.relation.foreach(commit_id, |value, count| {
            self.values.add(value, count);
        });
    }
}

pub struct Output<T, C = Box<dyn DynOp<T>>> {
    inner: RefCell<OutputInner<T, C>>,
    commit_id: Rc<Cell<CommitId>>,
}

pub type SavedOutput<T> = Output<T, SavedOp<T, Box<dyn DynOp<T>>>>;

impl<T, C> Output<T, C> {
    pub(crate) fn new(relation: RelationInner<T, C>, commit_id: Rc<Cell<CommitId>>) -> Self {
        Output {
            inner: RefCell::new(OutputInner {
                relation,
                values: E1Map::new(),
            }),
            commit_id,
        }
    }

    pub fn get(&self) -> Ref<E1Map<T, ValueCount>>
    where
        T: Eq + Hash,
        C: Op<T>,
    {
        self.inner.borrow_mut().update(self.commit_id.get());
        Ref::map(self.inner.borrow(), |inner| &inner.values)
    }
}

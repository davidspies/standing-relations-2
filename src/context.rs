use std::{cell::Cell, rc::Rc};

use uuid::Uuid;

use crate::{
    channel,
    operators::input::{Input, InputOp},
    relation::Relation,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ContextId(Uuid);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct CommitId(usize);

pub struct CreationContext {
    id: ContextId,
}

impl CreationContext {
    pub fn new() -> Self {
        Self {
            id: ContextId(Uuid::new_v4()),
        }
    }
    pub fn input<T>(&self) -> (Input<T>, Relation<T, InputOp<T>>) {
        let (sender, receiver) = channel::new();
        (
            Input::new(sender),
            Relation::new(self.id, InputOp::new(receiver)),
        )
    }
    pub fn begin(self) -> ExecutionContext {
        ExecutionContext {
            id: self.id,
            next_commit_id: Rc::new(Cell::new(1)),
        }
    }
}

pub struct ExecutionContext {
    id: ContextId,
    next_commit_id: Rc<Cell<usize>>,
}

impl ExecutionContext {
    pub fn commit(&mut self) {
        self.next_commit_id.set(self.next_commit_id.get() + 1);
    }
}

use uuid::Uuid;

use crate::{
    channel,
    operators::input::{Input, InputOp},
    relation::Relation,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ContextId(Uuid);

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
        ExecutionContext { id: self.id }
    }
}

pub struct ExecutionContext {
    id: ContextId,
}

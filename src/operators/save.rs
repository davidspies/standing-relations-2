use std::{cell::RefCell, rc::Rc, sync::Arc};

use crate::{
    broadcast_channel::{Receiver, Sender},
    context::{CommitId, ContextId},
    op::{DynOp, Op},
    relation::{data::RelationData, Relation, RelationInner},
    value_count::ValueCount,
};

struct SavedInner<T, C> {
    context_id: ContextId,
    data: Arc<RelationData>,
    last_id: CommitId,
    sub_rel: RelationInner<T, C>,
    sender: Sender<(T, ValueCount)>,
}

pub struct Saved<T, C = Box<dyn DynOp<T>>>(Rc<RefCell<SavedInner<T, C>>>);

impl<T, C> Saved<T, C> {
    pub(crate) fn new(sub_rel: Relation<T, C>) -> Self {
        let sender = Sender::new();
        Self(Rc::new(RefCell::new(SavedInner {
            context_id: sub_rel.context_id,
            data: Arc::new(sub_rel.data),
            last_id: CommitId::default(),
            sub_rel: sub_rel.inner,
            sender,
        })))
    }
}

pub struct SavedOp<T, C> {
    inner: Rc<RefCell<SavedInner<T, C>>>,
    receiver: Receiver<(T, ValueCount)>,
}

impl<T: Clone, C: Op<T>> Saved<T, C> {
    pub fn get(&self) -> Relation<T, SavedOp<T, C>> {
        let mut inner = self.0.borrow_mut();
        let receiver = inner.sender.subscribe();
        let operator = SavedOp {
            inner: self.0.clone(),
            receiver,
        };
        Relation::new(
            inner.context_id,
            RelationData::new(Op::type_name(&operator), vec![inner.data.clone()]),
            operator,
        )
        .hidden()
    }
}

impl<T: Clone, C: Op<T>> Op<T> for SavedOp<T, C> {
    fn type_name(&self) -> &'static str {
        "save"
    }
    fn foreach<F: FnMut(T, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        let mut inner = self.inner.borrow_mut();
        let SavedInner {
            context_id: _,
            data: _,
            sub_rel,
            sender,
            last_id,
        } = &mut *inner;
        if *last_id < current_id {
            sub_rel.send_to_broadcast(current_id, sender);
            *last_id = current_id
        }
        while let Some((t, count)) = self.receiver.try_recv() {
            f(t, count)
        }
    }
}

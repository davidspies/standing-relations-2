use std::sync::Arc;

use parking_lot::RwLock;

use crate::{
    broadcast_channel::{Receiver, Sender},
    commit_id::CommitId,
    op::Op,
    relation::Relation,
    value_count::ValueCount,
};

struct SavedInner<T, C> {
    last_id: CommitId,
    sub_rel: Relation<T, C>,
    sender: Sender<(T, ValueCount)>,
}

pub struct Saved<T, C>(Arc<RwLock<SavedInner<T, C>>>);

impl<T, C> Saved<T, C> {
    pub(crate) fn new(sub_rel: Relation<T, C>) -> Self {
        let sender = Sender::new();
        Self(Arc::new(RwLock::new(SavedInner {
            last_id: CommitId::default(),
            sub_rel,
            sender,
        })))
    }
}

pub struct SavedOp<T, C> {
    inner: Arc<RwLock<SavedInner<T, C>>>,
    receiver: Receiver<(T, ValueCount)>,
}

impl<T, C> Saved<T, C> {
    pub fn get(&self) -> Relation<T, SavedOp<T, C>> {
        let receiver = self.0.write().sender.receiver();
        Relation::new(SavedOp {
            inner: self.0.clone(),
            receiver,
        })
    }
}

impl<T: Clone, C: Op<T>> Op<T> for SavedOp<T, C> {
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        if self.inner.read().last_id < current_id {
            let mut inner = self.inner.write();
            let SavedInner {
                sub_rel,
                sender,
                last_id,
            } = &mut *inner;
            if *last_id < current_id {
                sub_rel.foreach(current_id, |t, count| sender.send((t, count)));
                *last_id = current_id
            }
        }
        self.receiver.try_iter().for_each(|(t, count)| f(t, count))
    }
}

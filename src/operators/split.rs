use std::{cell::RefCell, rc::Rc};

use crate::{
    channel::{self, Receiver, Sender},
    context::{CommitId, Ids},
    op::Op,
    relation::RelationInner,
    value_count::ValueCount,
};

struct SplitInner<L, R, C> {
    last_id: CommitId,
    sub_rel: RelationInner<(L, R), C>,
    left_sender: Sender<(L, Ids, ValueCount)>,
    right_sender: Sender<(R, Ids, ValueCount)>,
}

pub(crate) struct Split<L, R, C> {
    pub(crate) left: SplitOp<L, L, R, C>,
    pub(crate) right: SplitOp<R, L, R, C>,
}

impl<L, R, C> Split<L, R, C> {
    pub(crate) fn new(sub_rel: RelationInner<(L, R), C>) -> Self {
        let (left_sender, left_receiver) = channel::new();
        let (right_sender, right_receiver) = channel::new();
        let inner = Rc::new(RefCell::new(SplitInner {
            last_id: CommitId::default(),
            sub_rel,
            left_sender,
            right_sender,
        }));
        Self {
            left: SplitOp {
                inner: inner.clone(),
                receiver: left_receiver,
            },
            right: SplitOp {
                inner,
                receiver: right_receiver,
            },
        }
    }
}

pub struct SplitOp<T, L, R, C> {
    inner: Rc<RefCell<SplitInner<L, R, C>>>,
    receiver: Receiver<(T, Ids, ValueCount)>,
}

impl<T, L, R, C: Op<(L, R)>> Op<T> for SplitOp<T, L, R, C> {
    fn type_name(&self) -> &'static str {
        "split"
    }
    fn foreach<F: FnMut(T, Ids, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        let mut inner = self.inner.borrow_mut();
        let SplitInner {
            sub_rel,
            left_sender,
            right_sender,
            last_id,
        } = &mut *inner;
        if *last_id < current_id {
            sub_rel.foreach(current_id, |(l, r), ids, count| {
                let _ = left_sender.send((l, ids, count));
                let _ = right_sender.send((r, ids, count));
            });
            *last_id = current_id
        }
        while let Some((t, ids, count)) = self.receiver.try_recv() {
            f(t, ids, count)
        }
    }
}

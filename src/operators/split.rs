use std::{cell::RefCell, rc::Rc};

use crate::{
    channel::{self, Receiver, Sender},
    commit_id::CommitId,
    op::Op,
    relation::Relation,
    value_count::ValueCount,
};

struct SplitInner<L, R, C> {
    last_id: CommitId,
    sub_rel: Relation<(L, R), C>,
    left_sender: Sender<(L, ValueCount)>,
    right_sender: Sender<(R, ValueCount)>,
}

pub(crate) struct Split<L, R, C> {
    pub(crate) left: SplitOp<L, L, R, C>,
    pub(crate) right: SplitOp<R, L, R, C>,
}

impl<L, R, C> Split<L, R, C> {
    pub(crate) fn new(sub_rel: Relation<(L, R), C>) -> Self {
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
    receiver: Receiver<(T, ValueCount)>,
}

impl<T, L, R, C: Op<(L, R)>> Op<T> for SplitOp<T, L, R, C> {
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        let mut inner = self.inner.borrow_mut();
        let SplitInner {
            sub_rel,
            left_sender,
            right_sender,
            last_id,
        } = &mut *inner;
        if *last_id < current_id {
            sub_rel.foreach(current_id, |(l, r), count| {
                let _ = left_sender.send((l, count));
                let _ = right_sender.send((r, count));
            });
            *last_id = current_id
        }
        self.receiver.try_for_each(|(t, count)| f(t, count))
    }
}
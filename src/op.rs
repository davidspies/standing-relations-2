use std::{
    hash::Hash,
    sync::atomic::{self, AtomicUsize},
};

use crate::{broadcast_channel, context::CommitId, value_count::ValueCount, E1Map};

pub trait Op<T> {
    fn type_name(&self) -> &'static str;
    fn foreach<F: FnMut(T, ValueCount)>(&mut self, current_id: CommitId, f: F);
    fn dump_to_map(
        &mut self,
        current_id: CommitId,
        visit_count: &AtomicUsize,
        map: &mut E1Map<T, ValueCount>,
    ) where
        T: Eq + Hash,
    {
        self.foreach(current_id, |x, v| {
            map.add(x, v);
            visit_count.fetch_add(1, atomic::Ordering::Relaxed);
        })
    }
    fn send_to_broadcast(
        &mut self,
        current_id: CommitId,
        visit_count: &AtomicUsize,
        broadcast: &mut broadcast_channel::Sender<(T, ValueCount)>,
    ) where
        T: Clone,
    {
        self.foreach(current_id, |x, v| {
            broadcast.send(&(x.clone(), v));
            visit_count.fetch_add(1, atomic::Ordering::Relaxed);
        })
    }
}

impl<T, C: Op<T> + ?Sized> Op<T> for Box<C> {
    fn type_name(&self) -> &'static str {
        self.as_ref().type_name()
    }
    fn foreach<F: FnMut(T, ValueCount)>(&mut self, current_id: CommitId, f: F) {
        self.as_mut().foreach(current_id, f)
    }
}

pub trait DynOp<T> {
    fn type_name(&self) -> &'static str;
    fn foreach(&mut self, current_id: CommitId, f: &mut dyn FnMut(T, ValueCount));
    fn dump_to_map(
        &mut self,
        current_id: CommitId,
        visit_count: &AtomicUsize,
        map: &mut E1Map<T, ValueCount>,
    ) where
        T: Eq + Hash;
    fn send_to_broadcast(
        &mut self,
        current_id: CommitId,
        visit_count: &AtomicUsize,
        broadcast: &mut broadcast_channel::Sender<(T, ValueCount)>,
    ) where
        T: Clone;
}

impl<T, C: Op<T>> DynOp<T> for C {
    fn type_name(&self) -> &'static str {
        Op::type_name(self)
    }
    fn foreach(&mut self, current_id: CommitId, f: &mut dyn FnMut(T, ValueCount)) {
        Op::foreach(self, current_id, f)
    }
    fn dump_to_map(
        &mut self,
        current_id: CommitId,
        visit_count: &AtomicUsize,
        map: &mut E1Map<T, ValueCount>,
    ) where
        T: Eq + Hash,
    {
        Op::dump_to_map(self, current_id, visit_count, map)
    }
    fn send_to_broadcast(
        &mut self,
        current_id: CommitId,
        visit_count: &AtomicUsize,
        broadcast: &mut broadcast_channel::Sender<(T, ValueCount)>,
    ) where
        T: Clone,
    {
        Op::send_to_broadcast(self, current_id, visit_count, broadcast)
    }
}

impl<T> Op<T> for dyn DynOp<T> + '_ {
    fn type_name(&self) -> &'static str {
        DynOp::type_name(self)
    }
    fn foreach<F: FnMut(T, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        DynOp::foreach(self, current_id, &mut f)
    }
    fn dump_to_map(
        &mut self,
        current_id: CommitId,
        visit_count: &AtomicUsize,
        map: &mut E1Map<T, ValueCount>,
    ) where
        T: Eq + Hash,
    {
        DynOp::dump_to_map(self, current_id, visit_count, map)
    }
    fn send_to_broadcast(
        &mut self,
        current_id: CommitId,
        visit_count: &AtomicUsize,
        broadcast: &mut broadcast_channel::Sender<(T, ValueCount)>,
    ) where
        T: Clone,
    {
        DynOp::send_to_broadcast(self, current_id, visit_count, broadcast)
    }
}

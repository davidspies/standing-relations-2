use std::{collections::HashMap, hash::Hash};

use crate::{
    broadcast_channel,
    context::{CommitId, Ids},
    entry::Entry,
    generic_map::AddMap,
    relation::RelationInfo,
    value_count::ValueCount,
};

pub trait Op<T> {
    fn type_name(&self) -> &'static str;
    fn foreach<F: FnMut(T, Ids, ValueCount)>(&mut self, current_id: CommitId, f: F);
    fn dump_to_map(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        map: &mut HashMap<T, ValueCount>,
    ) where
        T: Eq + Hash,
    {
        self.foreach(current_id, |x, ids, v| {
            map.add((x, v));
            info.visit(ids)
        })
    }
    fn dump_to_vec(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        vec: &mut Vec<Entry<T>>,
    ) {
        self.foreach(current_id, |x, ids, v| {
            vec.push(Entry::new(x, ids, v));
            info.visit(ids)
        })
    }
    fn send_to_broadcast(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        broadcast: &mut broadcast_channel::Sender<(T, Ids, ValueCount)>,
    ) where
        T: Clone,
    {
        self.foreach(current_id, |x, ids, v| {
            broadcast.send(&(x.clone(), ids, v));
            info.visit(ids)
        })
    }
}

impl<T, C: Op<T> + ?Sized> Op<T> for Box<C> {
    fn type_name(&self) -> &'static str {
        self.as_ref().type_name()
    }
    fn foreach<F: FnMut(T, Ids, ValueCount)>(&mut self, current_id: CommitId, f: F) {
        self.as_mut().foreach(current_id, f)
    }
    fn dump_to_map(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        map: &mut HashMap<T, ValueCount>,
    ) where
        T: Eq + Hash,
    {
        self.as_mut().dump_to_map(current_id, info, map)
    }
    fn dump_to_vec(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        vec: &mut Vec<Entry<T>>,
    ) {
        self.as_mut().dump_to_vec(current_id, info, vec)
    }
    fn send_to_broadcast(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        broadcast: &mut broadcast_channel::Sender<(T, Ids, ValueCount)>,
    ) where
        T: Clone,
    {
        self.as_mut().send_to_broadcast(current_id, info, broadcast)
    }
}

pub trait DynOp<T> {
    fn type_name(&self) -> &'static str;
    fn foreach(&mut self, current_id: CommitId, f: &mut dyn FnMut(T, Ids, ValueCount));
    fn dump_to_map(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        map: &mut HashMap<T, ValueCount>,
    ) where
        T: Eq + Hash;
    fn dump_to_vec(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        vec: &mut Vec<Entry<T>>,
    );
    fn send_to_broadcast(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        broadcast: &mut broadcast_channel::Sender<(T, Ids, ValueCount)>,
    ) where
        T: Clone;
}

impl<T, C: Op<T>> DynOp<T> for C {
    fn type_name(&self) -> &'static str {
        Op::type_name(self)
    }
    fn foreach(&mut self, current_id: CommitId, f: &mut dyn FnMut(T, Ids, ValueCount)) {
        Op::foreach(self, current_id, f)
    }
    fn dump_to_map(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        map: &mut HashMap<T, ValueCount>,
    ) where
        T: Eq + Hash,
    {
        Op::dump_to_map(self, current_id, info, map)
    }
    fn dump_to_vec(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        vec: &mut Vec<Entry<T>>,
    ) {
        Op::dump_to_vec(self, current_id, info, vec)
    }
    fn send_to_broadcast(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        broadcast: &mut broadcast_channel::Sender<(T, Ids, ValueCount)>,
    ) where
        T: Clone,
    {
        Op::send_to_broadcast(self, current_id, info, broadcast)
    }
}

impl<T> Op<T> for dyn DynOp<T> + '_ {
    fn type_name(&self) -> &'static str {
        DynOp::type_name(self)
    }
    fn foreach<F: FnMut(T, Ids, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        DynOp::foreach(self, current_id, &mut f)
    }
    fn dump_to_map(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        map: &mut HashMap<T, ValueCount>,
    ) where
        T: Eq + Hash,
    {
        DynOp::dump_to_map(self, current_id, info, map)
    }
    fn dump_to_vec(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        vec: &mut Vec<Entry<T>>,
    ) {
        DynOp::dump_to_vec(self, current_id, info, vec)
    }
    fn send_to_broadcast(
        &mut self,
        current_id: CommitId,
        info: &mut RelationInfo,
        broadcast: &mut broadcast_channel::Sender<(T, Ids, ValueCount)>,
    ) where
        T: Clone,
    {
        DynOp::send_to_broadcast(self, current_id, info, broadcast)
    }
}

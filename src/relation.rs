#![allow(clippy::type_complexity)]

use std::{
    collections::HashMap,
    convert::identity,
    hash::Hash,
    iter,
    marker::PhantomData,
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

use generic_map::{
    clear::Clear,
    rollover_map::{RolloverHashedMaxHeap, RolloverHashedMinHeap, RolloverMap},
    GenericMap,
};

use crate::{
    broadcast_channel,
    context::{CommitId, ContextId},
    entry::Entry,
    generic_map::SingletonMap,
    nullable::Nullable,
    op::{DynOp, Op},
    operators::{
        concat::Concat,
        consolidate::Consolidate,
        flat_map::FlatMap,
        join::InnerJoin,
        negate::Negate,
        reduce::Reduce,
        save::Saved,
        split::{Split, SplitOp},
    },
    value_count::ValueCount,
};

use self::{args::RelationArgs, data::RelationData};

mod args;

pub(crate) mod data;

pub struct Relation<T, C = Box<dyn DynOp<T>>> {
    pub(crate) data: RelationData,
    pub(crate) context_id: ContextId,
    pub(crate) inner: RelationInner<T, C>,
}

pub struct RelationInfo {
    visit_count: Arc<AtomicUsize>,
}

impl RelationInfo {
    pub(crate) fn visit(&mut self) {
        self.visit_count.fetch_add(1, atomic::Ordering::Relaxed);
    }
}

pub(crate) struct RelationInner<T, C> {
    phantom: PhantomData<T>,
    info: RelationInfo,
    operator: C,
}

impl<T, C: Op<T>> RelationInner<T, C> {
    pub(crate) fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        self.operator.foreach(current_id, |x, v| f(x, v))
    }

    pub(crate) fn send_to_broadcast(
        &mut self,
        current_id: CommitId,
        broadcast: &mut broadcast_channel::Sender<(T, ValueCount)>,
    ) where
        T: Clone,
    {
        self.operator
            .send_to_broadcast(current_id, &mut self.info, broadcast)
    }

    pub(crate) fn dump_to_vec(&mut self, current_id: CommitId, vec: &mut Vec<Entry<T>>) {
        self.operator.dump_to_vec(current_id, &mut self.info, vec);
    }

    pub(crate) fn dump_to_map(&mut self, current_id: CommitId, map: &mut HashMap<T, ValueCount>)
    where
        T: Eq + Hash,
    {
        self.operator.dump_to_map(current_id, &mut self.info, map)
    }
}

impl<T, C> Relation<T, C> {
    pub(crate) fn new(context_id: ContextId, data: RelationData, operator: C) -> Self {
        Self {
            context_id,
            inner: RelationInner {
                phantom: PhantomData,
                info: RelationInfo {
                    visit_count: data.visit_count.clone(),
                },
                operator,
            },
            data,
        }
    }

    #[track_caller]
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.data.set_name(name.into());
        self
    }

    #[track_caller]
    pub fn type_named(mut self, type_name: &'static str) -> Self {
        self.data.set_type_name(type_name);
        self
    }

    #[track_caller]
    pub fn hidden(mut self) -> Self {
        self.data.hide();
        self
    }
}

impl<T, C: Op<T>> Relation<T, C> {
    pub(crate) fn from_op<Subrels: RelationArgs>(
        subrels: Subrels,
        operator: impl FnOnce(Subrels::Inner) -> C,
    ) -> Self {
        let mut context_ids = RolloverMap::new();
        subrels.add_context_ids(&mut context_ids);
        let mut children = Vec::new();
        let inner = subrels.push_datas(&mut children);
        let op = operator(inner);
        let data = RelationData::new(op.type_name(), children);
        Self::new(context_ids.into_singleton().unwrap().0, data, op)
    }

    pub fn dynamic<'a>(self) -> Relation<T, Box<dyn DynOp<T> + 'a>>
    where
        C: 'a,
    {
        Relation::new(self.context_id, self.data, Box::new(self.inner.operator))
    }

    pub fn flat_map<U, G: Fn(T) -> I, I>(self, g: G) -> Relation<U, FlatMap<T, G, C>>
    where
        I: IntoIterator<Item = U>,
    {
        Relation::from_op(self, |r| FlatMap::new(r, g))
    }

    pub fn consolidate(self) -> Relation<T, Consolidate<T, C>>
    where
        T: Clone + Eq + Hash,
    {
        Relation::from_op(self, Consolidate::new)
    }

    pub fn consolidate_h(self) -> Relation<T, Consolidate<T, C>>
    where
        T: Clone + Eq + Hash,
    {
        self.consolidate().hidden()
    }

    pub fn concat<CR>(self, other: Relation<T, CR>) -> Relation<T, Consolidate<T, Concat<T, C, CR>>>
    where
        CR: Op<T>,
        T: Clone + Eq + Hash,
    {
        Relation::from_op((self, other), Concat::new).consolidate_h()
    }

    pub fn negate(self) -> Relation<T, Negate<T, C>> {
        Relation::from_op(self, Negate::new)
    }

    pub fn save(self) -> Saved<T, C> {
        Saved::new(self)
    }

    pub fn minus<CR>(
        self,
        other: Relation<T, CR>,
    ) -> Relation<T, Consolidate<T, Concat<T, C, Negate<T, CR>>>>
    where
        CR: Op<T>,
        T: Clone + Eq + Hash,
    {
        self.concat(other.negate().hidden()).type_named("minus")
    }

    pub fn distinct(self) -> Relation<T, impl Op<T>>
    where
        T: Eq + Hash + Clone,
    {
        self.map_h(|x| (x, ()))
            .reduce_gen(|_, _: &RolloverMap<(), ValueCount>| ())
            .fsts()
            .type_named("distinct")
    }

    pub fn flatten<U>(self) -> Relation<U, impl Op<U>>
    where
        T: IntoIterator<Item = U>,
    {
        self.flat_map(identity).type_named("flatten")
    }

    pub fn map<U>(self, f: impl Fn(T) -> U) -> Relation<U, impl Op<U>> {
        self.flat_map(move |x| iter::once(f(x))).type_named("map")
    }

    pub fn filter(self, f: impl Fn(&T) -> bool) -> Relation<T, impl Op<T>>
    where
        C: Op<T>,
    {
        self.flat_map(move |x| if f(&x) { Some(x) } else { None })
            .type_named("filter")
    }

    pub fn map_h<U>(self, f: impl Fn(T) -> U) -> Relation<U, impl Op<U>>
    where
        C: Op<T>,
    {
        self.map(f).hidden()
    }

    pub fn collect<'a>(self) -> Saved<T, Box<dyn DynOp<T> + 'a>>
    where
        C: Op<T> + 'a,
    {
        self.dynamic().save()
    }
}

impl<T: Eq + Hash + Clone, C: Op<T>> Relation<T, C> {
    pub fn intersection(self, other: Relation<T, impl Op<T>>) -> Relation<T, impl Op<T>> {
        self.map_h(|t| (t, ()))
            .join(other.map_h(|t| (t, ())))
            .map_h(|(t, (), ())| t)
            .type_named("intersection")
    }

    pub fn counts(self) -> Relation<(T, isize), impl Op<(T, isize)>> {
        self.map_h(|t| (t, ()))
            .reduce_gen(|_, vals: &RolloverMap<(), ValueCount>| {
                let ((), &count) = vals.get_singleton().unwrap();
                count.0
            })
            .type_named("counts")
    }

    pub fn global_max(self) -> Relation<T, impl Op<T>>
    where
        T: Clone + Ord,
    {
        self.map_h(|t| ((), t))
            .maxes()
            .map_h(|((), t)| t)
            .type_named("global_max")
    }

    pub fn global_min(self) -> Relation<T, impl Op<T>>
    where
        T: Clone + Ord,
    {
        self.map_h(|t| ((), t))
            .mins()
            .map_h(|((), t)| t)
            .type_named("global_min")
    }
}

impl<K, V, C> Relation<(K, V), C>
where
    K: Eq + Hash + Clone,
    V: Eq + Hash + Clone,
    C: Op<(K, V)>,
{
    pub fn join<VR, CR>(
        self,
        other: Relation<(K, VR), CR>,
    ) -> Relation<(K, V, VR), Consolidate<(K, V, VR), InnerJoin<K, V, C, VR, CR>>>
    where
        VR: Eq + Hash + Clone,
        CR: Op<(K, VR)>,
    {
        Relation::from_op((self, other), InnerJoin::new).consolidate_h()
    }

    pub fn reduce<Y, G: Fn(&K, &RolloverMap<V, ValueCount, 2>) -> Y>(
        self,
        g: G,
    ) -> Relation<(K, Y), Consolidate<(K, Y), Reduce<K, V, Y, G, RolloverMap<V, ValueCount, 2>, C>>>
    where
        Y: Eq + Hash + Clone,
    {
        self.reduce_gen(g)
    }

    fn reduce_gen<Y, M, G: Fn(&K, &M) -> Y>(
        self,
        g: G,
    ) -> Relation<(K, Y), Consolidate<(K, Y), Reduce<K, V, Y, G, M, C>>>
    where
        M: GenericMap<K = V, V = ValueCount> + Clear + Nullable,
        Y: Eq + Hash + Clone,
    {
        Relation::from_op(self, |r| Reduce::new(r, g)).consolidate_h()
    }

    pub fn semijoin(self, other: Relation<K, impl Op<K>>) -> Relation<(K, V), impl Op<(K, V)>> {
        self.join(other.map_h(|t| (t, ())))
            .map_h(|(k, v, ())| (k, v))
            .type_named("semijoin")
    }

    pub fn join_values<VR: Eq + Hash + Clone>(
        self,
        other: Relation<(K, VR), impl Op<(K, VR)>>,
    ) -> Relation<(V, VR), impl Op<(V, VR)>> {
        self.join(other).map_h(|(_k, vl, vr)| (vl, vr))
    }

    pub fn maxes(self) -> Relation<(K, V), impl Op<(K, V)>>
    where
        V: Clone + Ord,
    {
        self.reduce_gen(|_, vals: &RolloverHashedMaxHeap<V, ValueCount, 2>| {
            vals.max_key().unwrap().clone()
        })
        .type_named("maxes")
    }

    pub fn mins(self) -> Relation<(K, V), impl Op<(K, V)>>
    where
        V: Clone + Ord,
    {
        self.reduce_gen(|_, vals: &RolloverHashedMinHeap<V, ValueCount, 2>| {
            vals.min_key().unwrap().clone()
        })
        .type_named("mins")
    }
}

impl<L, R, C: Op<(L, R)>> Relation<(L, R), C> {
    pub fn split(
        self,
    ) -> (
        Relation<L, SplitOp<L, L, R, C>>,
        Relation<R, SplitOp<R, L, R, C>>,
    ) {
        let context_id = self.context_id;
        let children = vec![Arc::new(self.data)];
        let Split { left, right } = Split::new(self.inner);
        (
            Relation::new(
                context_id,
                RelationData::new(Op::type_name(&left), children.clone()),
                left,
            )
            .hidden(),
            Relation::new(
                context_id,
                RelationData::new(Op::type_name(&right), children),
                right,
            )
            .hidden(),
        )
    }
    pub fn fsts(self) -> Relation<L, impl Op<L>>
    where
        C: Op<(L, R)>,
    {
        self.map_h(|(l, _r)| l)
    }
    pub fn snds(self) -> Relation<R, impl Op<R>>
    where
        C: Op<(L, R)>,
    {
        self.map_h(|(_l, r)| r)
    }
    pub fn swaps(self) -> Relation<(R, L), impl Op<(R, L)>>
    where
        C: Op<(L, R)>,
    {
        self.map_h(|(l, r)| (r, l))
    }
}

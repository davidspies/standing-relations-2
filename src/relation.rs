#![allow(clippy::type_complexity)]

use std::{
    convert::identity,
    hash::Hash,
    iter,
    marker::PhantomData,
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

use crate::{
    broadcast_channel,
    context::{CommitId, ContextId},
    e1map::{E1HashMaxHeap, E1HashMinHeap, E1Map},
    op::{DynOp, Op},
    operators::{
        antijoin::AntiJoin,
        concat::Concat,
        consolidate::Consolidate,
        distinct::Distinct,
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

pub(crate) struct RelationInner<T, C> {
    phantom: PhantomData<T>,
    visit_count: Arc<AtomicUsize>,
    operator: C,
}

impl<T, C: Op<T>> RelationInner<T, C> {
    pub(crate) fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        self.operator.foreach(current_id, |x, v| {
            self.visit_count.fetch_add(1, atomic::Ordering::Relaxed);
            f(x, v)
        })
    }

    pub(crate) fn send_to_broadcast(
        &mut self,
        current_id: CommitId,
        broadcast: &mut broadcast_channel::Sender<(T, ValueCount)>,
    ) where
        T: Clone,
    {
        self.operator
            .send_to_broadcast(current_id, &self.visit_count, broadcast)
    }
}

impl<T, C> Relation<T, C> {
    pub(crate) fn new(context_id: ContextId, data: RelationData, operator: C) -> Self {
        Self {
            context_id,
            inner: RelationInner {
                phantom: PhantomData,
                visit_count: data.visit_count.clone(),
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
        let mut context_ids = E1Map::new();
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

    pub fn distinct(self) -> Relation<T, Distinct<T, C>>
    where
        T: Eq + Hash + Clone,
    {
        Relation::from_op(self, Distinct::new)
    }

    pub fn consolidate(self) -> Relation<T, Consolidate<T, C>>
    where
        T: Eq + Hash,
    {
        Relation::from_op(self, Consolidate::new)
    }

    pub fn concat<CR>(self, other: Relation<T, CR>) -> Relation<T, Concat<T, C, CR>>
    where
        CR: Op<T>,
    {
        Relation::from_op((self, other), Concat::new)
    }

    pub fn negate(self) -> Relation<T, Negate<T, C>> {
        Relation::from_op(self, Negate::new)
    }

    pub fn save(self) -> Saved<T, C> {
        Saved::new(self)
    }

    pub fn minus<CR>(self, other: Relation<T, CR>) -> Relation<T, Concat<T, C, Negate<T, CR>>>
    where
        CR: Op<T>,
    {
        self.concat(other.negate().hidden()).type_named("minus")
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

    pub fn set_minus(self, other: Relation<T, impl Op<T>>) -> Relation<T, impl Op<T>> {
        self.map_h(|t| (t, ()))
            .antijoin(other)
            .fsts()
            .type_named("set_minus")
    }

    pub fn counts(self) -> Relation<(T, isize), impl Op<(T, isize)>> {
        self.map_h(|t| (t, ()))
            .reduce(|_, vals| {
                let ((), &count) = vals.get_singleton().unwrap();
                count
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
    ) -> Relation<(K, V, VR), InnerJoin<K, V, C, VR, CR>>
    where
        VR: Eq + Hash + Clone,
        CR: Op<(K, VR)>,
    {
        Relation::from_op((self, other), InnerJoin::new)
    }

    pub fn reduce<Y, G: Fn(&K, &E1Map<V, ValueCount>) -> Y>(
        self,
        g: G,
    ) -> Relation<(K, Y), Reduce<K, V, Y, G, E1Map<V, ValueCount>, C>>
    where
        Y: Eq + Clone,
    {
        Relation::from_op(self, |r| Reduce::new(r, g))
    }

    pub fn antijoin<CR: Op<K>>(
        self,
        other: Relation<K, CR>,
    ) -> Relation<(K, V), AntiJoin<K, V, C, CR>> {
        Relation::from_op((self, other), AntiJoin::new)
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
        Relation::from_op(self, |r| {
            Reduce::new(r, |_: &K, vals: &E1HashMaxHeap<V, ValueCount>| {
                let (v, _) = vals.max_key_value().unwrap();
                v.clone()
            })
        })
        .type_named("maxes")
    }

    pub fn mins(self) -> Relation<(K, V), impl Op<(K, V)>>
    where
        V: Clone + Ord,
    {
        Relation::from_op(self, |r| {
            Reduce::new(r, |_: &K, vals: &E1HashMinHeap<V, ValueCount>| {
                let (v, _) = vals.min_key_value().unwrap();
                v.clone()
            })
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

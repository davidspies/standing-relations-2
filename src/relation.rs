#![allow(clippy::type_complexity)]

use std::{convert::identity, hash::Hash, iter, marker::PhantomData, sync::Arc};

use crate::{
    context::{CommitId, ContextId},
    e1map::E1Map,
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
    phantom: PhantomData<T>,
    context_id: ContextId,
    data: Arc<RelationData>,
    operator: C,
}

impl<T, C> Relation<T, C> {
    pub(crate) fn new(context_id: ContextId, data: Arc<RelationData>, operator: C) -> Self {
        Self {
            phantom: PhantomData,
            context_id,
            data,
            operator,
        }
    }

    pub(crate) fn context_id(&self) -> ContextId {
        self.context_id
    }

    pub(crate) fn data(&self) -> Arc<RelationData> {
        self.data.clone()
    }
}

impl<T, C: Op<T>> Relation<T, C> {
    pub(crate) fn from_op<Subrels: RelationArgs>(
        subrels: Subrels,
        operator: impl FnOnce(Subrels) -> C,
    ) -> Self {
        let mut context_ids = E1Map::new();
        subrels.add_context_ids(&mut context_ids);
        let mut children = Vec::new();
        subrels.push_datas(&mut children);
        let op = operator(subrels);
        let data = Arc::new(RelationData::new(op.type_name(), children));
        Self::new(context_ids.into_singleton().unwrap().0, data, op)
    }

    pub(crate) fn foreach(&mut self, current_id: CommitId, f: impl FnMut(T, ValueCount)) {
        self.operator.foreach(current_id, f)
    }

    pub fn dynamic<'a>(self) -> Relation<T, Box<dyn DynOp<T> + 'a>>
    where
        C: 'a,
    {
        Relation::new(self.context_id, self.data, Box::new(self.operator))
    }

    pub fn flat_map<U, F: Fn(T) -> I, I>(self, f: F) -> Relation<U, FlatMap<T, F, C>>
    where
        I: IntoIterator<Item = U>,
    {
        Relation::from_op(self, |r| FlatMap::new(r, f))
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
        self.concat(other.negate())
    }

    pub fn flatten<U>(self) -> Relation<U, FlatMap<T, fn(T) -> T, C>>
    where
        T: IntoIterator<Item = U>,
    {
        self.flat_map(identity)
    }

    pub fn map<U>(self, f: impl Fn(T) -> U) -> Relation<U, impl Op<U>>
    where
        C: Op<T>,
    {
        self.flat_map(move |x| iter::once(f(x)))
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
        self.map(|t| (t, ()))
            .join(other.map(|t| (t, ())))
            .map(|(t, (), ())| t)
    }

    pub fn set_minus(self, other: Relation<T, impl Op<T>>) -> Relation<T, impl Op<T>> {
        self.map(|t| (t, ())).antijoin(other).fsts()
    }

    pub fn counts(self) -> Relation<(T, isize), impl Op<(T, isize)>> {
        self.map(|t| (t, ()))
            .reduce(|_: &T, vals: &E1Map<(), ValueCount>| {
                let ((), &count) = vals.get_singleton().unwrap();
                count
            })
    }

    pub fn global_max(self) -> Relation<T, impl Op<T>>
    where
        T: Ord,
    {
        self.map(|t| ((), t)).maxes().map(|((), t)| t)
    }

    pub fn global_min(self) -> Relation<T, impl Op<T>>
    where
        T: Ord,
    {
        self.map(|t| ((), t)).mins().map(|((), t)| t)
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

    pub fn reduce<Y, F: Fn(&K, &E1Map<V, ValueCount>) -> Y>(
        self,
        f: F,
    ) -> Relation<(K, Y), Reduce<K, V, Y, F, C>>
    where
        Y: Eq + Clone,
    {
        Relation::from_op(self, |r| Reduce::new(r, f))
    }

    pub fn antijoin<CR: Op<K>>(
        self,
        other: Relation<K, CR>,
    ) -> Relation<(K, V), AntiJoin<K, V, C, CR>> {
        Relation::from_op((self, other), AntiJoin::new)
    }

    pub fn semijoin(self, other: Relation<K, impl Op<K>>) -> Relation<(K, V), impl Op<(K, V)>> {
        self.join(other.map(|t| (t, ()))).map(|(k, v, ())| (k, v))
    }

    pub fn maxes(self) -> Relation<(K, V), impl Op<(K, V)>>
    where
        V: Ord,
    {
        self.reduce(|_: &K, vals: &E1Map<V, ValueCount>| {
            vals.iter().map(|(v, _)| v).max().unwrap().clone()
        })
    }

    pub fn mins(self) -> Relation<(K, V), impl Op<(K, V)>>
    where
        V: Ord,
    {
        self.reduce(|_: &K, vals: &E1Map<V, ValueCount>| {
            vals.iter().map(|(v, _)| v).min().unwrap().clone()
        })
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
        let children = vec![self.data()];
        let Split { left, right } = Split::new(self);
        (
            Relation::new(
                context_id,
                Arc::new(RelationData::new(Op::type_name(&left), children.clone())),
                left,
            ),
            Relation::new(
                context_id,
                Arc::new(RelationData::new(Op::type_name(&right), children)),
                right,
            ),
        )
    }
    pub fn fsts(self) -> Relation<L, impl Op<L>>
    where
        C: Op<(L, R)>,
    {
        self.map(|(l, _r)| l)
    }
    pub fn snds(self) -> Relation<R, impl Op<R>>
    where
        C: Op<(L, R)>,
    {
        self.map(|(_l, r)| r)
    }
    pub fn swaps(self) -> Relation<(R, L), impl Op<(R, L)>>
    where
        C: Op<(L, R)>,
    {
        self.map(|(l, r)| (r, l))
    }
}

#![allow(clippy::type_complexity)]

use std::{convert::identity, hash::Hash, iter, marker::PhantomData};

use crate::{
    commit_id::CommitId,
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

pub struct Relation<T, C> {
    phantom: PhantomData<T>,
    operator: C,
}

impl<T, C> Relation<T, C> {
    pub(crate) fn new(operator: C) -> Self {
        Self {
            phantom: PhantomData,
            operator,
        }
    }

    pub(crate) fn foreach(&mut self, current_id: CommitId, f: impl FnMut(T, ValueCount))
    where
        C: Op<T>,
    {
        self.operator.foreach(current_id, f)
    }

    pub fn dynamic<'a>(self) -> Relation<T, Box<dyn DynOp<T> + 'a>>
    where
        C: Op<T> + 'a,
    {
        Relation::new(Box::new(self.operator))
    }

    pub fn flat_map<U, F>(self, f: F) -> Relation<U, FlatMap<T, F, C>> {
        Relation::new(FlatMap::new(self, f))
    }

    pub fn distinct(self) -> Relation<T, Distinct<T, C>> {
        Relation::new(Distinct::new(self))
    }

    pub fn consolidate(self) -> Relation<T, Consolidate<T, C>> {
        Relation::new(Consolidate::new(self))
    }

    pub fn concat<CR>(self, other: Relation<T, CR>) -> Relation<T, Concat<T, C, CR>> {
        Relation::new(Concat::new(self, other))
    }

    pub fn negate(self) -> Relation<T, Negate<T, C>> {
        Relation::new(Negate::new(self))
    }

    pub fn save(self) -> Saved<T, C> {
        Saved::new(self)
    }

    pub fn minus<CR>(self, other: Relation<T, CR>) -> Relation<T, Concat<T, C, Negate<T, CR>>> {
        self.concat(other.negate())
    }

    pub fn flatten<U>(self) -> Relation<U, FlatMap<T, fn(T) -> T, C>> {
        self.flat_map(identity)
    }

    pub fn map<U>(self, f: impl Fn(T) -> U) -> Relation<U, impl Op<U>>
    where
        C: Op<T>,
    {
        self.flat_map(move |x| iter::once(f(x)))
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
                let ((), count) = vals.iter().next().unwrap();
                count.count()
            })
    }
}

impl<K, V, C> Relation<(K, V), C> {
    pub fn join<VR, CR>(
        self,
        other: Relation<(K, VR), CR>,
    ) -> Relation<(K, V, VR), InnerJoin<K, V, C, VR, CR>> {
        Relation::new(InnerJoin::new(self, other))
    }

    pub fn reduce<Y, F>(self, f: F) -> Relation<(K, Y), Reduce<K, V, Y, F, C>> {
        Relation::new(Reduce::new(self, f))
    }

    pub fn antijoin<CR: Op<K>>(
        self,
        other: Relation<K, CR>,
    ) -> Relation<(K, V), AntiJoin<K, V, C, CR>> {
        Relation::new(AntiJoin::new(self, other))
    }

    pub fn semijoin(self, other: Relation<K, impl Op<K>>) -> Relation<(K, V), impl Op<(K, V)>>
    where
        K: Eq + Hash + Clone,
        V: Eq + Hash + Clone,
        C: Op<(K, V)>,
    {
        self.join(other.map(|t| (t, ()))).map(|(k, v, ())| (k, v))
    }
}

impl<L, R, C> Relation<(L, R), C> {
    pub fn split(
        self,
    ) -> (
        Relation<L, SplitOp<L, L, R, C>>,
        Relation<R, SplitOp<R, L, R, C>>,
    ) {
        let Split { left, right } = Split::new(self);
        (Relation::new(left), Relation::new(right))
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
}

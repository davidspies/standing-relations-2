#![allow(clippy::type_complexity)]

use std::{convert::identity, hash::Hash, iter, marker::PhantomData};

use crate::{
    e1map::E1Map,
    op::{DynOp, Op},
    operators::{
        concat::Concat, consolidate::Consolidate, distinct::Distinct, flat_map::FlatMap,
        join::InnerJoin, negate::Negate, reduce::Reduce,
    },
    value_count::ValueCount,
};

pub struct Relation<T, C> {
    phantom: PhantomData<T>,
    inner: C,
}

impl<T, C> Relation<T, C> {
    pub(crate) fn foreach(&mut self, f: impl FnMut(T, ValueCount))
    where
        C: Op<T>,
    {
        self.inner.foreach(f)
    }

    pub fn dynamic<'a>(self) -> Relation<T, Box<dyn DynOp<T> + 'a>>
    where
        C: Op<T> + 'a,
    {
        Relation {
            phantom: self.phantom,
            inner: Box::new(self.inner),
        }
    }

    pub fn flat_map<U, F>(self, f: F) -> Relation<U, FlatMap<T, F, C>> {
        Relation {
            phantom: PhantomData,
            inner: FlatMap::new(self, f),
        }
    }

    pub fn distinct(self) -> Relation<T, Distinct<T, C>> {
        Relation {
            phantom: PhantomData,
            inner: Distinct::new(self),
        }
    }

    pub fn consolidate(self) -> Relation<T, Consolidate<T, C>> {
        Relation {
            phantom: PhantomData,
            inner: Consolidate::new(self),
        }
    }

    pub fn concat<CR>(self, other: Relation<T, CR>) -> Relation<T, Concat<T, C, CR>> {
        Relation {
            phantom: PhantomData,
            inner: Concat::new(self, other),
        }
    }

    pub fn negate(self) -> Relation<T, Negate<T, C>> {
        Relation {
            phantom: PhantomData,
            inner: Negate::new(self),
        }
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

    pub fn intersection<CR: Op<T>>(self, other: Relation<T, CR>) -> Relation<T, impl Op<T>>
    where
        T: Eq + Hash + Clone,
        C: Op<T>,
    {
        self.map(|t| (t, ()))
            .join(other.map(|t| (t, ())))
            .map(|(t, (), ())| t)
    }

    pub fn counts(self) -> Relation<(T, isize), impl Op<(T, isize)>>
    where
        T: Eq + Hash + Clone,
        C: Op<T>,
    {
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
        Relation {
            phantom: PhantomData,
            inner: InnerJoin::new(self, other),
        }
    }

    pub fn reduce<Y, F>(self, f: F) -> Relation<(K, Y), Reduce<K, V, Y, F, C>> {
        Relation {
            phantom: PhantomData,
            inner: Reduce::new(self, f),
        }
    }

    pub fn semijoin(self, other: Relation<K, impl Op<K>>) -> Relation<(K, V), impl Op<(K, V)>>
    where
        K: Eq + Hash + Clone,
        V: Eq + Hash + Clone,
        C: Op<(K, V)>,
    {
        self.join(other.map(|t| (t, ()))).map(|(k, v, ())| (k, v))
    }

    pub fn fsts(self) -> Relation<K, impl Op<K>>
    where
        C: Op<(K, V)>,
    {
        self.map(|(k, _v)| k)
    }
    pub fn snds(self) -> Relation<V, impl Op<V>>
    where
        C: Op<(K, V)>,
    {
        self.map(|(_k, v)| v)
    }
}

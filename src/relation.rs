use std::marker::PhantomData;

use crate::{
    op::{DynOp, Op},
    operators::{consolidate::Consolidate, distinct::Distinct, flat_map::FlatMap, join::InnerJoin},
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

    pub fn flat_map<U, I: IntoIterator<Item = U>, F: FnMut(T) -> I>(
        self,
        f: F,
    ) -> Relation<U, FlatMap<T, F, C>> {
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
}

impl<K, VL, C> Relation<(K, VL), C> {
    pub fn join<VR, CR>(
        self,
        other: Relation<(K, VR), CR>,
    ) -> Relation<(K, VL, VR), InnerJoin<K, VL, C, VR, CR>> {
        Relation {
            phantom: PhantomData,
            inner: InnerJoin::new(self, other),
        }
    }
}

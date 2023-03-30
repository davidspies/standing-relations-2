use std::{
    hash::{Hash, Hasher},
    ops::Deref,
    ptr,
    sync::Arc,
};

use derivative::Derivative;

#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub(crate) struct ArcKey<T>(pub(crate) Arc<T>);

impl<T> PartialEq for ArcKey<T> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl<T> Eq for ArcKey<T> {}

impl<T> Hash for ArcKey<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ptr::hash(Arc::as_ptr(&self.0), state)
    }
}

impl<T> Deref for ArcKey<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

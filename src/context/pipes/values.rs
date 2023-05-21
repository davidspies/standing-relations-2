use std::collections::{HashMap, HashSet};

use derivative::Derivative;

use crate::ValueCount;

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
pub(super) struct Values<T> {
    pub(super) values: HashMap<T, ValueCount>,
    pub(super) seen: HashSet<T>,
}

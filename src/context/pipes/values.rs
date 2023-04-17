use std::collections::HashMap;

use derivative::Derivative;

use crate::{context::DataId, ValueCount};

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
pub(super) struct Values<T> {
    pub(super) values: HashMap<T, ValueCount>,
    pub(super) seen: HashMap<T, DataId>,
}

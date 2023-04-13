use std::{collections::HashMap, hash::Hash};

use generic_map::rollover_map::RolloverMap;

use crate::{
    context::CommitId, generic_map::AddMap, op::Op, relation::RelationInner,
    value_count::ValueCount,
};

pub struct Distinct<T, C> {
    sub_rel: RelationInner<T, C>,
    current_counts: RolloverMap<T, ValueCount>,
    changed_scratch: HashMap<T, DistinctChange>,
}

impl<T, C> Distinct<T, C> {
    pub(crate) fn new(sub_rel: RelationInner<T, C>) -> Self {
        Self {
            sub_rel,
            current_counts: RolloverMap::default(),
            changed_scratch: HashMap::default(),
        }
    }
}

impl<T: Clone + Eq + Hash, C: Op<T>> Op<T> for Distinct<T, C> {
    fn type_name(&self) -> &'static str {
        "distinct"
    }
    fn foreach<F: FnMut(T, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.sub_rel.foreach(current_id, |value, count| {
            let was_zero = !self.current_counts.contains_key(&value);
            self.current_counts.add((value.clone(), count));
            let is_zero = !self.current_counts.contains_key(&value);
            if was_zero && !is_zero {
                self.changed_scratch.entry(value).or_default().add();
            } else if !was_zero && is_zero {
                self.changed_scratch.entry(value).or_default().remove();
            }
        });
        self.changed_scratch.drain().for_each(|(value, change)| {
            if let Some(count) = change.count() {
                f(value, count)
            }
        })
    }
}

#[derive(Clone, Copy, Default)]
enum DistinctChange {
    Removed,
    #[default]
    NoChange,
    Added,
}

impl DistinctChange {
    fn add(&mut self) {
        match self {
            Self::NoChange => *self = Self::Added,
            Self::Added => panic!("Already added"),
            Self::Removed => *self = Self::NoChange,
        }
    }
    fn remove(&mut self) {
        match self {
            Self::NoChange => *self = Self::Removed,
            Self::Added => *self = Self::NoChange,
            Self::Removed => panic!("Already removed"),
        }
    }
    fn count(&self) -> Option<ValueCount> {
        match self {
            Self::NoChange => None,
            Self::Added => Some(ValueCount(1)),
            Self::Removed => Some(ValueCount(-1)),
        }
    }
}

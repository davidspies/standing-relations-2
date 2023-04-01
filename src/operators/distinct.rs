use std::{collections::HashMap, hash::Hash};

use crate::{
    add_to_value::ValueChanges, context::CommitId, e1map::E1Map, op::Op, relation::RelationInner,
    value_count::ValueCount,
};

pub struct Distinct<T, C> {
    sub_rel: RelationInner<T, C>,
    current_counts: E1Map<T, ValueCount>,
    changed_scratch: HashMap<T, DistinctChange>,
}

impl<T, C> Distinct<T, C> {
    pub(crate) fn new(sub_rel: RelationInner<T, C>) -> Self {
        Self {
            sub_rel,
            current_counts: E1Map::default(),
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
            match self.current_counts.add(value.clone(), count) {
                ValueChanges {
                    was_zero: true,
                    is_zero: false,
                } => self.changed_scratch.entry(value).or_default().add(),
                ValueChanges {
                    was_zero: false,
                    is_zero: true,
                } => self.changed_scratch.entry(value).or_default().remove(),
                ValueChanges {
                    was_zero: true,
                    is_zero: true,
                } => panic!("zero count"),
                ValueChanges {
                    was_zero: false,
                    is_zero: false,
                } => (),
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
    fn count(&self) -> Option<isize> {
        match self {
            Self::NoChange => None,
            Self::Added => Some(1),
            Self::Removed => Some(-1),
        }
    }
}

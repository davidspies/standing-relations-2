use std::{collections::HashMap, hash::Hash};

use crate::{
    add_to_value::ValueChanges, context::CommitId, e1map::E1Map, op::Op, relation::Relation,
    value_count::ValueCount,
};

pub struct Distinct<T, C> {
    sub_rel: Relation<T, C>,
    current_counts: E1Map<T, ValueCount>,
    changed_scratch: HashMap<T, DistinctChange>,
}

impl<T, C> Distinct<T, C> {
    pub fn new(sub_rel: Relation<T, C>) -> Self {
        Self {
            sub_rel,
            current_counts: E1Map::default(),
            changed_scratch: HashMap::default(),
        }
    }
}

impl<T: Clone + Eq + Hash, C: Op<T>> Op<T> for Distinct<T, C> {
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        self.sub_rel.foreach(current_id, |value, count| {
            match self.current_counts.add(value.clone(), count) {
                ValueChanges {
                    was_zero: true,
                    is_zero: false,
                } => self
                    .changed_scratch
                    .entry(value)
                    .or_default()
                    .add(count.commit_id),
                ValueChanges {
                    was_zero: false,
                    is_zero: true,
                } => self
                    .changed_scratch
                    .entry(value)
                    .or_default()
                    .remove(count.commit_id),
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

#[derive(Default)]
struct DistinctChange {
    commit_id: CommitId,
    value: DistinctChangeValue,
}

impl DistinctChange {
    fn add(&mut self, context: CommitId) {
        self.commit_id = self.commit_id.max(context);
        self.value.add()
    }
    fn remove(&mut self, commit_id: CommitId) {
        self.commit_id = self.commit_id.max(commit_id);
        self.value.remove()
    }

    fn count(&self) -> Option<ValueCount> {
        match self.value {
            DistinctChangeValue::Removed => Some(ValueCount::decr(self.commit_id)),
            DistinctChangeValue::NoChange => None,
            DistinctChangeValue::Added => Some(ValueCount::incr(self.commit_id)),
        }
    }
}

#[derive(Clone, Copy, Default)]
enum DistinctChangeValue {
    Removed,
    #[default]
    NoChange,
    Added,
}

impl DistinctChangeValue {
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
}

use std::{collections::HashMap, hash::Hash};

use crate::{commit_id::CommitId, op::Op, relation::Relation, value_count::ValueCount};

#[derive(Default)]
struct DistinctChange {
    commit_id: CommitId,
    value: DistinctChangeValue,
}

impl DistinctChange {
    fn add(&mut self, commit_id: CommitId) {
        self.commit_id = self.commit_id.max(commit_id);
        self.value.add()
    }
    fn remove(&mut self, commit_id: CommitId) {
        self.commit_id = self.commit_id.max(commit_id);
        self.value.remove()
    }

    fn count(&self) -> ValueCount {
        ValueCount {
            commit_id: self.commit_id,
            count: self.value.count(),
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

    fn count(&self) -> isize {
        match self {
            DistinctChangeValue::Removed => -1,
            DistinctChangeValue::NoChange => 0,
            DistinctChangeValue::Added => 1,
        }
    }
}

pub struct Distinct<T, C> {
    sub_rel: Relation<T, C>,
    current_counts: HashMap<T, ValueCount>,
    changed: HashMap<T, DistinctChange>,
}

impl<T: Clone + Eq + Hash, C: Op<T>> Op<T> for Distinct<T, C> {
    fn foreach(&mut self, mut f: impl FnMut(T, ValueCount)) {
        self.sub_rel.foreach(|value, count| {
            let cur_count = self.current_counts.entry(value.clone()).or_default();
            let was_zero = cur_count.count == 0;
            *cur_count += count;
            let is_zero = cur_count.count == 0;
            let commit_id = cur_count.commit_id;
            if is_zero {
                self.current_counts.remove(&value);
            }
            match (was_zero, is_zero) {
                (true, false) => self.changed.entry(value).or_default().add(commit_id),
                (false, true) => self.changed.entry(value).or_default().remove(commit_id),
                (true, true) => panic!("zero count"),
                (false, false) => (),
            }
        });
        self.changed.drain().for_each(|(value, change)| {
            let count = change.count();
            if count.count != 0 {
                f(value, count)
            }
        })
    }
}

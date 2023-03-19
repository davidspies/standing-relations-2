use std::ops::{Add, AddAssign, Mul};

use crate::{
    add_to_value::{AddToValue, ValueChanges},
    commit_id::CommitId,
    nullable::Nullable,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct ValueCount {
    pub(crate) commit_id: CommitId,
    pub(crate) count: isize,
}

impl Add for ValueCount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            commit_id: self.commit_id.max(rhs.commit_id),
            count: self.count + rhs.count,
        }
    }
}

impl AddAssign for ValueCount {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs
    }
}

impl Mul for ValueCount {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        Self {
            commit_id: self.commit_id.max(rhs.commit_id),
            count: self.count * rhs.count,
        }
    }
}

impl Nullable for ValueCount {
    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl AddToValue<ValueCount> for ValueCount {
    fn add_to(self, v: &mut ValueCount) -> ValueChanges {
        let was_zero = v.count == 0;
        *v += self;
        let is_zero = v.count == 0;
        ValueChanges { was_zero, is_zero }
    }
}

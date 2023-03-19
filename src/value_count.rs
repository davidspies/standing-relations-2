use std::ops::{Add, AddAssign, Mul};

use crate::commit_id::CommitId;

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

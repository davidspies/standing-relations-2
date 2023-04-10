use std::ops::{AddAssign, Mul, Neg};

use generic_map::clear::Clear;

use crate::nullable::Nullable;

#[derive(Default, Clone, Copy, Debug)]
pub struct ValueCount(pub isize);

impl Clear for ValueCount {
    fn clear(&mut self) {
        self.0 = 0;
    }
}

impl Nullable for ValueCount {
    fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

impl Neg for ValueCount {
    type Output = Self;

    fn neg(self) -> Self::Output {
        ValueCount(-self.0)
    }
}

impl AddAssign for ValueCount {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

impl Mul for ValueCount {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        ValueCount(self.0 * rhs.0)
    }
}

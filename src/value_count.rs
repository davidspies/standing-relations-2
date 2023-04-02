use std::ops::{AddAssign, Mul, Neg, SubAssign};

use generic_map::clear::Clear;
#[cfg(feature = "redis")]
use redis::ToRedisArgs;

use crate::nullable::Nullable;

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValueCount(pub isize);
impl ValueCount {
    pub fn min_magnitude(&self, other: Self) -> Self {
        if self.0.abs() < other.0.abs() {
            *self
        } else {
            other
        }
    }

    pub fn signum(&self) -> isize {
        self.0.signum()
    }
}

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

impl SubAssign for ValueCount {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
    }
}

impl Mul for ValueCount {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        ValueCount(self.0 * rhs.0)
    }
}

#[cfg(feature = "redis")]
impl ToRedisArgs for ValueCount {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        self.0.write_redis_args(out)
    }
}

use std::ops::Deref;

use generic_map::{clear::Clear, rollover_map::RolloverHashedMaxHeap};

use crate::{context::Level, generic_map::AddMap, nullable::Nullable, ValueCount};

#[derive(Default)]
pub(super) struct TrackedLevels<M> {
    map: M,
    level_tracker: RolloverHashedMaxHeap<Level, ValueCount, 2>,
}
impl<M> TrackedLevels<M> {
    pub(crate) fn current_level(&self) -> Option<Level> {
        self.level_tracker.top_key().copied()
    }
}

impl<M> Deref for TrackedLevels<M> {
    type Target = M;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<M: Clear> Clear for TrackedLevels<M> {
    fn clear(&mut self) {
        self.map.clear();
        self.level_tracker.clear();
    }
}

impl<M: Nullable> Nullable for TrackedLevels<M> {
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<V, M: AddMap<(V, ValueCount)>> AddMap<((V, Level), ValueCount)> for TrackedLevels<M> {
    fn add(&mut self, ((v, lvl), count): ((V, Level), ValueCount)) {
        self.map.add((v, count));
        self.level_tracker.add((lvl, count));
    }
}

#![allow(clippy::type_complexity)]

use std::hash::Hash;

use crate::{
    context::CommitId, op::Op, relation::RelationInner, rollover_map::RolloverMap,
    value_count::ValueCount,
};

pub struct InnerJoin<K, VL, CL, VR, CR> {
    left_rel: RelationInner<(K, VL), CL>,
    right_rel: RelationInner<(K, VR), CR>,
    left_values: RolloverMap<K, RolloverMap<VL, ValueCount>>,
    right_values: RolloverMap<K, RolloverMap<VR, ValueCount>>,
}

impl<K, VL, CL, VR, CR> InnerJoin<K, VL, CL, VR, CR> {
    pub(crate) fn new(
        (left_rel, right_rel): (RelationInner<(K, VL), CL>, RelationInner<(K, VR), CR>),
    ) -> Self {
        Self {
            left_rel,
            right_rel,
            left_values: RolloverMap::default(),
            right_values: RolloverMap::default(),
        }
    }
}

impl<K, VL, CL, VR, CR> Op<(K, VL, VR)> for InnerJoin<K, VL, CL, VR, CR>
where
    K: Eq + Hash + Clone,
    VL: Eq + Hash + Clone,
    VR: Eq + Hash + Clone,
    CL: Op<(K, VL)>,
    CR: Op<(K, VR)>,
{
    fn type_name(&self) -> &'static str {
        "join"
    }
    fn foreach<F: FnMut((K, VL, VR), ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.left_rel.foreach(current_id, |(k, vl), lcount| {
            for (vr, &rcount) in self.right_values.get(&k).into_iter().flatten() {
                f((k.clone(), vl.clone(), vr.clone()), lcount * rcount)
            }
            self.left_values.add(k, (vl, lcount));
        });
        self.right_rel.foreach(current_id, |(k, vr), rcount| {
            for (vl, &lcount) in self.left_values.get(&k).into_iter().flatten() {
                f((k.clone(), vl.clone(), vr.clone()), lcount * rcount)
            }
            self.right_values.add(k, (vr, rcount));
        });
    }
}

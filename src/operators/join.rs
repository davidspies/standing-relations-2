use std::hash::Hash;

use crate::{
    commit_id::CommitId, e1map::E1Map, op::Op, relation::Relation, value_count::ValueCount,
};

pub struct InnerJoin<K, VL, CL, VR, CR> {
    left_rel: Relation<(K, VL), CL>,
    right_rel: Relation<(K, VR), CR>,
    left_values: E1Map<K, E1Map<VL, ValueCount>>,
    right_values: E1Map<K, E1Map<VR, ValueCount>>,
}

impl<K, VL, CL, VR, CR> InnerJoin<K, VL, CL, VR, CR> {
    pub fn new(left_rel: Relation<(K, VL), CL>, right_rel: Relation<(K, VR), CR>) -> Self {
        assert_eq!(left_rel.context_id(), right_rel.context_id());
        Self {
            left_rel,
            right_rel,
            left_values: E1Map::default(),
            right_values: E1Map::default(),
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
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut((K, VL, VR), ValueCount)) {
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

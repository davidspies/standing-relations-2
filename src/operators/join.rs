use std::hash::Hash;

use crate::{e1map::E1Map, op::Op, relation::Relation, value_count::ValueCount};

pub struct InnerJoin<K, VL, CL, VR, CR> {
    left_rel: Relation<(K, VL), CL>,
    right_rel: Relation<(K, VR), CR>,
    left_values: E1Map<K, E1Map<VL, ValueCount>>,
    right_values: E1Map<K, E1Map<VR, ValueCount>>,
}

impl<K, VL, CL, VR, CR> Op<(K, VL, VR)> for InnerJoin<K, VL, CL, VR, CR>
where
    K: Eq + Hash + Clone,
    VL: Eq + Hash + Clone,
    VR: Eq + Hash + Clone,
    CL: Op<(K, VL)>,
    CR: Op<(K, VR)>,
{
    fn foreach(&mut self, mut f: impl FnMut((K, VL, VR), ValueCount)) {
        self.left_rel.foreach(|(k, vl), lcount| {
            if let Some(vrs) = self.right_values.get(&k) {
                for (vr, &rcount) in vrs.iter() {
                    f((k.clone(), vl.clone(), vr.clone()), lcount * rcount)
                }
            }
            self.left_values.add(k, (vl, lcount));
        });
        self.right_rel.foreach(|(k, vr), rcount| {
            if let Some(vls) = self.left_values.get(&k) {
                for (vl, &lcount) in vls.iter() {
                    f((k.clone(), vl.clone(), vr.clone()), lcount * rcount)
                }
            }
            self.right_values.add(k, (vr, rcount));
        });
    }
}

#![allow(clippy::type_complexity)]

use std::{collections::HashMap, hash::Hash};

use generic_map::rollover_map::RolloverMap;

use crate::{
    context::CommitId, generic_map::AddMap, op::Op, relation::RelationInner,
    value_count::ValueCount,
};

pub struct InnerJoin<K, VL, CL, VR, CR> {
    left_rel: RelationInner<(K, VL), CL>,
    right_rel: RelationInner<(K, VR), CR>,
    left_values: HashMap<K, RolloverMap<VL, ValueCount, 2>>,
    right_values: HashMap<K, RolloverMap<VR, ValueCount, 2>>,
}

impl<K, VL, CL, VR, CR> InnerJoin<K, VL, CL, VR, CR> {
    pub(crate) fn new(
        (left_rel, right_rel): (RelationInner<(K, VL), CL>, RelationInner<(K, VR), CR>),
    ) -> Self {
        Self {
            left_rel,
            right_rel,
            left_values: HashMap::default(),
            right_values: HashMap::default(),
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
            self.left_values.add((k, (vl, lcount)));
        });
        self.right_rel.foreach(current_id, |(k, vr), rcount| {
            for (vl, &lcount) in self.left_values.get(&k).into_iter().flatten() {
                f((k.clone(), vl.clone(), vr.clone()), lcount * rcount)
            }
            self.right_values.add((k, (vr, rcount)));
        });
    }
}

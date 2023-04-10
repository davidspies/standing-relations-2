use std::hash::Hash;

use generic_map::rollover_map::RolloverMap;

use crate::{
    context::CommitId, generic_map::AddMap, op::Op, relation::RelationInner,
    value_count::ValueCount,
};

pub struct AntiJoin<K, V, CL, CR> {
    left_rel: RelationInner<(K, V), CL>,
    right_rel: RelationInner<K, CR>,
    left_values: RolloverMap<K, RolloverMap<V, ValueCount>>,
    right_values: RolloverMap<K, ValueCount>,
}

impl<K, V, CL, CR> AntiJoin<K, V, CL, CR> {
    pub(crate) fn new(
        (left_rel, right_rel): (RelationInner<(K, V), CL>, RelationInner<K, CR>),
    ) -> Self {
        Self {
            left_rel,
            right_rel,
            left_values: RolloverMap::default(),
            right_values: RolloverMap::default(),
        }
    }
}

impl<K, V, CL, CR> Op<(K, V)> for AntiJoin<K, V, CL, CR>
where
    K: Eq + Hash + Clone,
    V: Eq + Hash + Clone,
    CL: Op<(K, V)>,
    CR: Op<K>,
{
    fn type_name(&self) -> &'static str {
        "anti_join"
    }
    fn foreach<F: FnMut((K, V), ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.right_rel.foreach(current_id, |k, count| {
            let was_zero = !self.right_values.contains_key(&k);
            self.right_values.add((k.clone(), count));
            let is_zero = !self.right_values.contains_key(&k);
            if was_zero && !is_zero {
                if let Some(left_vals) = self.left_values.get(&k) {
                    for (v, count) in left_vals.iter() {
                        f((k.clone(), v.clone()), -*count);
                    }
                }
            } else if !was_zero && is_zero {
                if let Some(left_vals) = self.left_values.get(&k) {
                    for (v, count) in left_vals.iter() {
                        f((k.clone(), v.clone()), *count);
                    }
                }
            }
        });
        self.left_rel.foreach(current_id, |(k, v), count| {
            if !self.right_values.contains_key(&k) {
                f((k.clone(), v.clone()), count);
            }
            self.left_values.add((k, (v, count)));
        })
    }
}

use std::hash::Hash;

use crate::{
    add_to_value::ValueChanges, context::CommitId, op::Op, relation::RelationInner,
    rollover_map::RolloverMap, value_count::ValueCount,
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
            match self.right_values.add(k.clone(), count) {
                ValueChanges {
                    was_zero: true,
                    is_zero: false,
                } => {
                    for (v, &lcount) in self.left_values.get(&k).into_iter().flatten() {
                        f((k.clone(), v.clone()), -lcount)
                    }
                }
                ValueChanges {
                    was_zero: false,
                    is_zero: true,
                } => {
                    for (v, &lcount) in self.left_values.get(&k).into_iter().flatten() {
                        f((k.clone(), v.clone()), lcount)
                    }
                }
                ValueChanges {
                    was_zero: true,
                    is_zero: true,
                } => panic!("zero count"),
                ValueChanges {
                    was_zero: false,
                    is_zero: false,
                } => (),
            }
        });
        self.left_rel.foreach(current_id, |(k, v), count| {
            if !self.right_values.contains_key(&k) {
                f((k.clone(), v.clone()), count);
            }
            self.left_values.add(k, (v, count));
        })
    }
}

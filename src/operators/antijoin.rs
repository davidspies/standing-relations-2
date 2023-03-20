use std::hash::Hash;

use crate::{
    add_to_value::ValueChanges, commit_id::CommitId, e1map::E1Map, op::Op, relation::Relation,
    value_count::ValueCount,
};

pub struct AntiJoin<K, V, CL, CR> {
    left_rel: Relation<(K, V), CL>,
    right_rel: Relation<K, CR>,
    left_values: E1Map<K, E1Map<V, ValueCount>>,
    right_values: E1Map<K, ValueCount>,
}

impl<K, V, CL, CR> AntiJoin<K, V, CL, CR> {
    pub fn new(left_rel: Relation<(K, V), CL>, right_rel: Relation<K, CR>) -> Self {
        Self {
            left_rel,
            right_rel,
            left_values: E1Map::default(),
            right_values: E1Map::default(),
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
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut((K, V), ValueCount)) {
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
            if self.right_values.get(&k).is_none() {
                f((k.clone(), v.clone()), count);
            }
            self.left_values.add(k, (v, count));
        })
    }
}

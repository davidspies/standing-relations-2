use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
    mem,
};

use crate::{context::CommitId, e1map::E1Map, op::Op, relation::Relation, value_count::ValueCount};

pub struct Reduce<K, V, Y, F, C> {
    sub_rel: Relation<(K, V), C>,
    f: F,
    aggregated_values: E1Map<K, E1Map<V, ValueCount>>,
    outputs: HashMap<K, (CommitId, Y)>,
    changed_keys_scratch: HashMap<K, CommitId>,
}

impl<K, V, Y, F, C> Reduce<K, V, Y, F, C> {
    pub(crate) fn new(sub_rel: Relation<(K, V), C>, f: F) -> Self {
        Self {
            sub_rel,
            f,
            aggregated_values: E1Map::default(),
            outputs: HashMap::default(),
            changed_keys_scratch: HashMap::default(),
        }
    }
}

impl<K, V, Y, F, C> Op<(K, Y)> for Reduce<K, V, Y, F, C>
where
    K: Eq + Hash + Clone,
    V: Eq + Hash,
    Y: Eq + Clone,
    F: Fn(&K, &E1Map<V, ValueCount>) -> Y,
    C: Op<(K, V)>,
{
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut((K, Y), ValueCount)) {
        self.sub_rel.foreach(current_id, |(k, v), count| {
            self.aggregated_values.add(k.clone(), (v, count));
            let context = self.changed_keys_scratch.entry(k).or_default();
            *context = (*context).max(count.context);
        });
        for (k, context) in self.changed_keys_scratch.drain() {
            match self.aggregated_values.get(&k) {
                None => {
                    if let Some((context, y)) = self.outputs.remove(&k) {
                        f((k, y), ValueCount::decr(context))
                    }
                }
                Some(vals) => {
                    let new_y = (self.f)(&k, vals);
                    match self.outputs.entry(k.clone()) {
                        hash_map::Entry::Vacant(vac) => {
                            vac.insert((context, new_y.clone()));
                            f((k, new_y), ValueCount::incr(context));
                        }
                        hash_map::Entry::Occupied(mut occ) => {
                            let out = occ.get_mut();
                            if new_y != out.1 {
                                let (_, old_y) = mem::replace(out, (context, new_y.clone()));
                                f((k.clone(), old_y), ValueCount::decr(context));
                                f((k, new_y), ValueCount::incr(context));
                            }
                        }
                    }
                }
            }
        }
    }
}

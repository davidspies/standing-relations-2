use std::{
    collections::{hash_map, HashMap, HashSet},
    hash::Hash,
    mem,
};

use crate::{
    add_to_value::AddToValue, context::CommitId, e1map::E1Map, nullable::Nullable, op::Op,
    relation::RelationInner, value_count::ValueCount,
};

pub struct Reduce<K, V, Y, G, M, C> {
    sub_rel: RelationInner<(K, V), C>,
    g: G,
    aggregated_values: E1Map<K, M>,
    outputs: HashMap<K, Y>,
    changed_keys_scratch: HashSet<K>,
}

impl<K, V, Y, G, M: Default, C> Reduce<K, V, Y, G, M, C> {
    pub(crate) fn new(sub_rel: RelationInner<(K, V), C>, g: G) -> Self {
        Self {
            sub_rel,
            g,
            aggregated_values: E1Map::default(),
            outputs: HashMap::default(),
            changed_keys_scratch: HashSet::default(),
        }
    }
}

impl<K, V, Y, G, M, C> Op<(K, Y)> for Reduce<K, V, Y, G, M, C>
where
    K: Eq + Hash + Clone,
    V: Eq + Hash,
    Y: Eq + Clone,
    G: Fn(&K, &M) -> Y,
    M: Nullable,
    (V, ValueCount): AddToValue<M>,
    C: Op<(K, V)>,
{
    fn type_name(&self) -> &'static str {
        "reduce"
    }
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut((K, Y), ValueCount)) {
        self.sub_rel.foreach(current_id, |(k, v), count| {
            self.aggregated_values.add(k.clone(), (v, count));
            self.changed_keys_scratch.insert(k);
        });
        for k in self.changed_keys_scratch.drain() {
            match self.aggregated_values.get(&k) {
                None => {
                    if let Some(y) = self.outputs.remove(&k) {
                        f((k, y), -1)
                    }
                }
                Some(vals) => {
                    let new_y = (self.g)(&k, vals);
                    match self.outputs.entry(k.clone()) {
                        hash_map::Entry::Vacant(vac) => {
                            vac.insert(new_y.clone());
                            f((k, new_y), 1);
                        }
                        hash_map::Entry::Occupied(mut occ) => {
                            let out = occ.get_mut();
                            if new_y != *out {
                                let old_y = mem::replace(out, new_y.clone());
                                f((k.clone(), old_y), -1);
                                f((k, new_y), 1);
                            }
                        }
                    }
                }
            }
        }
    }
}

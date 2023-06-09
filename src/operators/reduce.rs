use std::{
    collections::{hash_map, HashMap, HashSet},
    hash::Hash,
    mem,
};

use generic_map::{GenericMap, RolloverMap};

use crate::{
    context::CommitId, entry::Entry, generic_map::AddMap, op::Op, relation::RelationInner,
    value_count::ValueCount,
};

pub struct Reduce<K, V, Y, G, M, C> {
    sub_rel: RelationInner<(K, V), C>,
    g: G,
    aggregated_values: RolloverMap<K, M>,
    outputs: HashMap<K, Y>,
    encountered_changes_scratch: Vec<Entry<(K, V)>>,
    changed_keys_scratch: HashSet<K>,
}

impl<K, V, Y, G: Fn(&K, &M) -> Y, M: Default, C> Reduce<K, V, Y, G, M, C> {
    pub(crate) fn new(sub_rel: RelationInner<(K, V), C>, g: G) -> Self {
        Self {
            sub_rel,
            g,
            aggregated_values: RolloverMap::new(),
            outputs: HashMap::new(),
            encountered_changes_scratch: Vec::new(),
            changed_keys_scratch: HashSet::new(),
        }
    }
}

impl<K, V, Y, G, M, C> Op<(K, Y)> for Reduce<K, V, Y, G, M, C>
where
    K: Eq + Hash + Clone,
    V: Eq + Hash,
    Y: Eq + Clone,
    G: Fn(&K, &M) -> Y,
    M: GenericMap<K = V, V = ValueCount> + AddMap<(V, ValueCount)>,
    C: Op<(K, V)>,
{
    fn type_name(&self) -> &'static str {
        "reduce"
    }
    fn foreach<F: FnMut((K, Y), ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.sub_rel
            .dump_to_vec(current_id, &mut self.encountered_changes_scratch);
        for e in self.encountered_changes_scratch.drain(..) {
            let Entry {
                value: (k, v),
                value_count,
            } = e;
            self.changed_keys_scratch.insert(k.clone());
            self.aggregated_values.add((k, (v, value_count)));
        }
        for k in self.changed_keys_scratch.drain() {
            match self.aggregated_values.get(&k) {
                None => {
                    if let Some(y) = self.outputs.remove(&k) {
                        f((k, y), ValueCount(-1))
                    }
                }
                Some(vals) => {
                    let new_y = (self.g)(&k, vals);
                    match self.outputs.entry(k.clone()) {
                        hash_map::Entry::Vacant(vac) => {
                            vac.insert(new_y.clone());
                            f((k, new_y), ValueCount(1));
                        }
                        hash_map::Entry::Occupied(mut occ) => {
                            let out = occ.get_mut();
                            if new_y != *out {
                                let old_y = mem::replace(out, new_y.clone());
                                f((k.clone(), old_y), ValueCount(-1));
                                f((k, new_y), ValueCount(1));
                            }
                        }
                    }
                }
            }
        }
    }
}

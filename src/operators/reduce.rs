use std::{
    collections::{hash_map, HashMap, HashSet},
    hash::Hash,
    mem,
};

use generic_map::{GenericMap, RolloverMap};

use crate::{
    context::{CommitId, Level},
    entry::Entry,
    generic_map::AddMap,
    op::Op,
    relation::RelationInner,
    value_count::ValueCount,
};

use self::tracked_levels::TrackedLevels;

mod tracked_levels;

pub struct Reduce<K, V, Y, G, M, C> {
    sub_rel: RelationInner<(K, V), C>,
    g: G,
    aggregated_values: RolloverMap<K, TrackedLevels<M>>,
    outputs: HashMap<K, (Y, Level)>,
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
    fn foreach<F: FnMut((K, Y), Level, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.sub_rel
            .dump_to_vec(current_id, &mut self.encountered_changes_scratch);
        for e in self.encountered_changes_scratch.drain(..) {
            let Entry {
                value: (k, v),
                level,
                value_count,
            } = e;
            self.changed_keys_scratch.insert(k.clone());
            self.aggregated_values.add((k, ((v, level), value_count)));
        }
        for k in self.changed_keys_scratch.drain() {
            match self.aggregated_values.get(&k) {
                None => {
                    if let Some((old_y, old_level)) = self.outputs.remove(&k) {
                        f((k, old_y), old_level, ValueCount(-1))
                    }
                }
                Some(vals) => {
                    let new_y = (self.g)(&k, vals);
                    let new_level = vals.current_level().unwrap();
                    match self.outputs.entry(k.clone()) {
                        hash_map::Entry::Vacant(vac) => {
                            vac.insert((new_y.clone(), new_level));
                            f((k, new_y), new_level, ValueCount(1));
                        }
                        hash_map::Entry::Occupied(mut occ) => {
                            let (out, cur_level) = occ.get_mut();
                            if new_y != *out || new_level != *cur_level {
                                let old_y = mem::replace(out, new_y.clone());
                                let old_level = mem::replace(cur_level, new_level);
                                f((k.clone(), old_y), old_level, ValueCount(-1));
                                f((k, new_y), new_level, ValueCount(1));
                            }
                        }
                    }
                }
            }
        }
    }
}

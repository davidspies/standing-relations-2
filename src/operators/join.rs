#![allow(clippy::type_complexity)]

use std::{collections::HashMap, hash::Hash};

use generic_map::rollover_map::RolloverMap;

use crate::{
    context::{CommitId, Ids},
    entry::Entry,
    generic_map::AddMap,
    op::Op,
    relation::RelationInner,
    value_count::ValueCount,
};

pub struct InnerJoin<K, VL, CL, VR, CR> {
    left_rel: RelationInner<(K, VL), CL>,
    right_rel: RelationInner<(K, VR), CR>,
    left_values: HashMap<K, RolloverMap<VL, ValueCount, 2>>,
    right_values: HashMap<K, RolloverMap<VR, ValueCount, 2>>,
    left_scratch: Vec<Entry<(K, VL)>>,
    right_scratch: Vec<Entry<(K, VR)>>,
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
            left_scratch: Vec::new(),
            right_scratch: Vec::new(),
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
    fn foreach<F: FnMut((K, VL, VR), Ids, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.left_rel
            .dump_to_vec(current_id, &mut self.left_scratch);
        self.right_rel
            .dump_to_vec(current_id, &mut self.right_scratch);
        let mut left_iter = self.left_scratch.drain(..).peekable();
        let mut right_iter = self.right_scratch.drain(..).peekable();
        loop {
            match (left_iter.peek(), right_iter.peek()) {
                (Some(left), Some(right)) => {
                    if left.ids <= right.ids {
                        let Entry {
                            value: (k, vl),
                            ids,
                            value_count: lcount,
                        } = left_iter.next().unwrap();
                        for (vr, &rcount) in self.right_values.get(&k).into_iter().flatten() {
                            f((k.clone(), vl.clone(), vr.clone()), ids, lcount * rcount)
                        }
                        self.left_values.add((k, (vl, lcount)));
                    } else {
                        let Entry {
                            value: (k, vr),
                            ids,
                            value_count: rcount,
                        } = right_iter.next().unwrap();
                        for (vl, &lcount) in self.left_values.get(&k).into_iter().flatten() {
                            f((k.clone(), vl.clone(), vr.clone()), ids, lcount * rcount)
                        }
                        self.right_values.add((k, (vr, rcount)));
                    }
                    continue;
                }
                (_, None) => {
                    for entry in left_iter {
                        let Entry {
                            value: (k, vl),
                            ids,
                            value_count: lcount,
                        } = entry;
                        for (vr, &rcount) in self.right_values.get(&k).into_iter().flatten() {
                            f((k.clone(), vl.clone(), vr.clone()), ids, lcount * rcount)
                        }
                        self.left_values.add((k, (vl, lcount)));
                    }
                }
                (None, _) => {
                    for entry in right_iter {
                        let Entry {
                            value: (k, vr),
                            ids,
                            value_count: rcount,
                        } = entry;
                        for (vl, &lcount) in self.left_values.get(&k).into_iter().flatten() {
                            f((k.clone(), vl.clone(), vr.clone()), ids, lcount * rcount)
                        }
                        self.right_values.add((k, (vr, rcount)));
                    }
                }
            }
            break;
        }
    }
}

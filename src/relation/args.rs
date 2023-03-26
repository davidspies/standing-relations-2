use std::sync::Arc;

use crate::{context::ContextId, e1map::E1Map, relation::Relation};

use super::data::RelationData;

pub(crate) trait RelationArgs {
    fn add_context_ids(&self, s: &mut E1Map<ContextId, isize>);
    fn push_datas(&self, _v: &mut Vec<Arc<RelationData>>);
}

impl RelationArgs for () {
    fn add_context_ids(&self, _s: &mut E1Map<ContextId, isize>) {}
    fn push_datas(&self, _v: &mut Vec<Arc<RelationData>>) {}
}

impl<T, C> RelationArgs for Relation<T, C> {
    fn add_context_ids(&self, s: &mut E1Map<ContextId, isize>) {
        s.add(self.context_id(), 1);
    }
    fn push_datas(&self, v: &mut Vec<Arc<RelationData>>) {
        v.push(self.data.clone());
    }
}

impl<A, B> RelationArgs for (A, B)
where
    A: RelationArgs,
    B: RelationArgs,
{
    fn add_context_ids(&self, s: &mut E1Map<ContextId, isize>) {
        let (a, b) = self;
        a.add_context_ids(s);
        b.add_context_ids(s);
    }
    fn push_datas(&self, v: &mut Vec<Arc<RelationData>>) {
        let (a, b) = self;
        a.push_datas(v);
        b.push_datas(v);
    }
}

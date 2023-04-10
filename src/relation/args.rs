use std::sync::Arc;

use generic_map::rollover_map::RolloverMap;

use crate::{context::ContextId, generic_map::AddMap, relation::Relation, value_count::ValueCount};

use super::{data::RelationData, RelationInner};

pub(crate) trait RelationArgs {
    type Inner;

    fn add_context_ids(&self, s: &mut RolloverMap<ContextId, ValueCount>);
    fn push_datas(self, _v: &mut Vec<Arc<RelationData>>) -> Self::Inner;
}

impl RelationArgs for ContextId {
    type Inner = ();

    fn add_context_ids(&self, s: &mut RolloverMap<ContextId, ValueCount>) {
        s.add((*self, ValueCount(1)));
    }
    fn push_datas(self, _v: &mut Vec<Arc<RelationData>>) -> Self::Inner {}
}

impl<T, C> RelationArgs for Relation<T, C> {
    type Inner = RelationInner<T, C>;

    fn add_context_ids(&self, s: &mut RolloverMap<ContextId, ValueCount>) {
        s.add((self.context_id, ValueCount(1)));
    }
    fn push_datas(self, v: &mut Vec<Arc<RelationData>>) -> Self::Inner {
        v.push(Arc::new(self.data));
        self.inner
    }
}

impl<A, B> RelationArgs for (A, B)
where
    A: RelationArgs,
    B: RelationArgs,
{
    type Inner = (A::Inner, B::Inner);

    fn add_context_ids(&self, s: &mut RolloverMap<ContextId, ValueCount>) {
        let (a, b) = self;
        a.add_context_ids(s);
        b.add_context_ids(s);
    }
    fn push_datas(self, v: &mut Vec<Arc<RelationData>>) -> Self::Inner {
        let (a, b) = self;
        let a_inner = a.push_datas(v);
        let b_inner = b.push_datas(v);
        (a_inner, b_inner)
    }
}

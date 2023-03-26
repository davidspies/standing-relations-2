use crate::{context::ContextId, e1map::E1Map, relation::Relation};

pub(crate) trait RelationArgs {
    fn push_context_ids(&self, s: &mut E1Map<ContextId, isize>);
}

impl RelationArgs for () {
    fn push_context_ids(&self, _s: &mut E1Map<ContextId, isize>) {}
}

impl<T, C> RelationArgs for Relation<T, C> {
    fn push_context_ids(&self, s: &mut E1Map<ContextId, isize>) {
        s.add(self.context_id(), 1);
    }
}

impl<A, B> RelationArgs for (A, B)
where
    A: RelationArgs,
    B: RelationArgs,
{
    fn push_context_ids(&self, s: &mut E1Map<ContextId, isize>) {
        let (a, b) = self;
        a.push_context_ids(s);
        b.push_context_ids(s);
    }
}

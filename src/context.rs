use std::cell::Cell;
use std::collections::HashSet;
#[cfg(feature = "redis")]
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;

use index_list::IndexList;
use uuid::Uuid;

use crate::{
    arc_key::ArcKey,
    channel,
    op::Op,
    operators::input::{Input, InputOp},
    output::Output,
    relation::{data::RelationData, Relation},
    value_count::ValueCount,
    who::Who,
};

#[cfg(feature = "redis")]
use self::pipes::redis::RedisPipe;
use self::pipes::{
    feedback::FeedbackPipe, interrupt::Interrupt, tracked::TrackedInputPipe,
    untracked::UntrackedInputPipe, PipeT, ProcessResult, Processable,
};

pub use self::pipes::interrupt::InterruptId;

mod pipes;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ContextId(Uuid);

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, Default, PartialOrd, Ord)]
pub struct CommitId(usize);

pub struct CreationContext<'a> {
    id: ContextId,
    commit_id: Rc<Cell<CommitId>>,
    input_pipes: Vec<Box<dyn PipeT + 'a>>,
    feedback_pipes: IndexList<Box<dyn Processable + 'a>>,
    relational_graph: HashSet<ArcKey<RelationData>>,
    #[cfg(feature = "redis")]
    redis: Option<redis::Client>,
}

impl<'a> Default for CreationContext<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> CreationContext<'a> {
    pub fn new() -> Self {
        Self {
            id: ContextId(Uuid::new_v4()),
            commit_id: Rc::new(Cell::new(CommitId(0))),
            input_pipes: Vec::new(),
            feedback_pipes: IndexList::new(),
            relational_graph: HashSet::new(),
            #[cfg(feature = "redis")]
            redis: None,
        }
    }
    #[cfg(feature = "redis")]
    pub fn with_redis(redis: redis::Client) -> Self {
        Self {
            redis: Some(redis),
            ..Self::new()
        }
    }
    pub fn input<T: Eq + Hash + Clone + 'a>(&mut self) -> (Input<T>, Relation<T, InputOp<T>>) {
        let (sender1, receiver1) = channel::new::<(T, Who)>();
        let (sender2, receiver2) = channel::new::<(T, ValueCount)>();
        self.input_pipes
            .push(Box::new(TrackedInputPipe::new(receiver1, sender2)));
        (
            Input::new(self.id, sender1),
            Relation::from_op(self.id, move |()| InputOp::new(receiver2)),
        )
    }
    pub fn frameless_input<T: Eq + Hash + Clone + 'a>(
        &mut self,
    ) -> (Input<T>, Relation<T, InputOp<T>>) {
        let (sender1, receiver1) = channel::new::<(T, Who)>();
        let (sender2, receiver2) = channel::new::<(T, ValueCount)>();
        self.input_pipes
            .push(Box::new(UntrackedInputPipe::new(receiver1, sender2)));
        (
            Input::new(self.id, sender1),
            Relation::from_op(self.id, move |()| InputOp::new(receiver2)),
        )
    }
    pub fn feedback<T: Eq + Hash + Clone + 'a>(
        &mut self,
        relation: Relation<T, impl Op<T> + 'a>,
        input: Input<T>,
    ) {
        assert_eq!(self.id, relation.context_id);
        assert_eq!(self.id, input.context_id);
        self.add_all(&Arc::new(relation.data));
        self.feedback_pipes
            .insert_last(Box::new(FeedbackPipe::new(relation.inner, input)));
    }
    pub fn interrupt<T: Eq + Hash + 'a, C: Op<T> + 'a>(
        &mut self,
        id: InterruptId,
        relation: Relation<T, C>,
    ) {
        assert_eq!(self.id, relation.context_id);
        self.add_all(&Arc::new(relation.data));
        self.feedback_pipes
            .insert_last(Box::new(Interrupt::new(id, relation.inner)));
    }
    pub fn output<T, C>(&mut self, relation: Relation<T, C>) -> Output<T, C> {
        assert_eq!(self.id, relation.context_id);
        self.add_all(&Arc::new(relation.data));
        Output::new(relation.inner, self.commit_id.clone())
    }
    #[cfg(feature = "redis")]
    pub fn send_to_redis<T, C>(&mut self, name: impl ToString, relation: Relation<T, C>)
    where
        T: Clone + Eq + Hash + Debug + 'a,
        C: Op<T> + 'a,
    {
        assert_eq!(self.id, relation.context_id);
        self.add_all(&Arc::new(relation.data));
        self.feedback_pipes.insert_last(Box::new(RedisPipe::new(
            name.to_string(),
            relation.inner,
            self.redis.clone().unwrap(),
        )));
    }
    pub fn begin(self) -> ExecutionContext<'a> {
        let Self {
            id: _,
            commit_id,
            input_pipes,
            feedback_pipes,
            ..
        } = self;
        ExecutionContext {
            commit_id,
            input_pipes,
            feedback_pipes,
        }
    }

    fn add_all(&mut self, data: &Arc<RelationData>) {
        if self.relational_graph.insert(ArcKey(data.clone())) {
            for child in data.children.iter() {
                self.add_all(child);
            }
        }
    }
}

pub struct ExecutionContext<'a> {
    commit_id: Rc<Cell<CommitId>>,
    input_pipes: Vec<Box<dyn PipeT + 'a>>,
    feedback_pipes: IndexList<Box<dyn Processable + 'a>>,
}

impl ExecutionContext<'_> {
    pub fn commit(&mut self) -> Result<(), InterruptId> {
        self.one_pass();
        'outer: loop {
            let commit_id = self.commit_id.get();
            let mut i = self.feedback_pipes.first_index();
            while i.is_some() {
                let next_i = self.feedback_pipes.next_index(i);
                match self.feedback_pipes.get_mut(i).unwrap().process(commit_id) {
                    Ok(ProcessResult::Changed) => {
                        self.one_pass();
                        continue 'outer;
                    }
                    Ok(ProcessResult::Unchanged) => {}
                    Ok(ProcessResult::Interrupted(interrupt_id)) => {
                        return Err(interrupt_id);
                    }
                    Err(Dropped) => {
                        self.feedback_pipes.remove(i);
                    }
                }
                i = next_i;
            }
            return Ok(());
        }
    }

    pub fn with_frame<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        self.one_pass();

        for input in self.input_pipes.iter_mut() {
            input.push_frame();
        }

        let result = f(self);

        self.input_pipes
            .retain_mut(|input| input.pop_frame(self.commit_id.get()).is_ok());

        self.commit_id.set(CommitId(self.commit_id.get().0 + 1));
        let mut i = self.feedback_pipes.first_index();
        while i.is_some() {
            let next_i = self.feedback_pipes.next_index(i);
            match self
                .feedback_pipes
                .get_mut(i)
                .unwrap()
                .process(self.commit_id.get())
            {
                Ok(_) => {}
                Err(Dropped) => {
                    self.feedback_pipes.remove(i);
                }
            }
            i = next_i;
        }
        self.input_pipes
            .retain_mut(|pipe| pipe.process(self.commit_id.get()).is_ok());

        result
    }

    fn one_pass(&mut self) {
        self.commit_id.set(CommitId(self.commit_id.get().0 + 1));
        self.input_pipes
            .retain_mut(|pipe| pipe.process(self.commit_id.get()).is_ok());
    }
}

#[derive(Debug)]
pub(crate) struct Dropped;

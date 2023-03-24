use std::hash::Hash;

use index_list::IndexList;
use uuid::Uuid;

use crate::{
    channel,
    operators::input::{Input, InputOp},
    relation::Relation,
    Op, ValueCount,
};

use self::pipes::{
    feedback::FeedbackPipe, tracked::TrackedInputPipe, untracked::UntrackedInputPipe, PipeT,
};

mod pipes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ContextId(Uuid);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct CommitId(usize);

pub struct CreationContext<'a> {
    id: ContextId,
    input_pipes: Vec<Box<dyn PipeT + 'a>>,
    feedback_pipes: IndexList<Box<dyn PipeT + 'a>>,
}

impl<'a> CreationContext<'a> {
    pub fn new() -> Self {
        Self {
            id: ContextId(Uuid::new_v4()),
            input_pipes: Vec::new(),
            feedback_pipes: IndexList::new(),
        }
    }
    pub fn input<T: Eq + Hash + Clone + 'a>(&mut self) -> (Input<T>, Relation<T, InputOp<T>>) {
        let (sender1, receiver1) = channel::new::<(T, isize)>();
        let (sender2, receiver2) = channel::new::<(T, ValueCount)>();
        self.input_pipes
            .push(Box::new(TrackedInputPipe::new(receiver1, sender2)));
        (
            Input::new(sender1),
            Relation::new(self.id, InputOp::new(receiver2)),
        )
    }
    pub fn frameless_input<T: 'a>(&mut self) -> (Input<T>, Relation<T, InputOp<T>>) {
        let (sender1, receiver1) = channel::new::<(T, isize)>();
        let (sender2, receiver2) = channel::new::<(T, ValueCount)>();
        self.input_pipes
            .push(Box::new(UntrackedInputPipe::new(receiver1, sender2)));
        (
            Input::new(sender1),
            Relation::new(self.id, InputOp::new(receiver2)),
        )
    }
    pub fn feedback<T: Eq + Hash + Clone + 'a>(
        &mut self,
        relation: Relation<T, impl Op<T> + 'a>,
        input: Input<T>,
    ) {
        self.feedback_pipes
            .insert_last(Box::new(FeedbackPipe::new(relation, input)));
    }
    pub fn begin(self) -> ExecutionContext<'a> {
        ExecutionContext {
            current_commit_id: 0,
            input_pipes: self.input_pipes,
            feedback_pipes: self.feedback_pipes,
        }
    }
}

pub struct ExecutionContext<'a> {
    current_commit_id: usize,
    input_pipes: Vec<Box<dyn PipeT + 'a>>,
    feedback_pipes: IndexList<Box<dyn PipeT + 'a>>,
}

impl ExecutionContext<'_> {
    pub fn commit(&mut self) {
        self.one_pass();
        'outer: loop {
            let commit_id = self.commit_id();
            let mut i = self.feedback_pipes.first_index();
            while i.is_some() {
                let next_i = self.feedback_pipes.next_index(i);
                match self.feedback_pipes.get_mut(i).unwrap().process(commit_id) {
                    Ok(true) => {
                        self.one_pass();
                        continue 'outer;
                    }
                    Ok(false) => {}
                    Err(Dropped) => {
                        self.feedback_pipes.remove(i);
                    }
                }
                i = next_i;
            }
            return;
        }
    }

    pub fn with_frame<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        let mut i = self.feedback_pipes.first_index();
        while i.is_some() {
            let next_i = self.feedback_pipes.next_index(i);
            self.feedback_pipes.get_mut(i).unwrap().push_frame();
            i = next_i;
        }

        for input in self.input_pipes.iter_mut() {
            input.push_frame();
        }

        let result = f(self);

        self.input_pipes
            .retain_mut(|input| input.pop_frame().is_ok());

        let mut i = self.feedback_pipes.first_index();
        while i.is_some() {
            let next_i = self.feedback_pipes.next_index(i);
            if self.feedback_pipes.get_mut(i).unwrap().pop_frame().is_err() {
                self.feedback_pipes.remove(i);
            }
            i = next_i;
        }

        self.one_pass();

        result
    }

    fn one_pass(&mut self) {
        self.current_commit_id += 1;
        let commit_id = self.commit_id();
        self.input_pipes
            .retain_mut(|pipe| pipe.process(commit_id).is_ok());
    }

    fn commit_id(&self) -> CommitId {
        CommitId(self.current_commit_id)
    }
}

#[derive(Debug)]
pub(crate) struct Dropped;

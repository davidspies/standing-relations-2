use std::hash::Hash;

use index_list::IndexList;
use uuid::Uuid;

use crate::{
    channel,
    operators::input::{Input, InputOp},
    relation::Relation,
    E1Map, Op, ValueCount,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ContextId(Uuid);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct CommitId(usize);

pub struct CreationContext<'a> {
    id: ContextId,
    input_pipes: Vec<Box<dyn InputPipeT + 'a>>,
    feedback_pipes: IndexList<Box<dyn FeedbackPipeT + 'a>>,
}

impl<'a> CreationContext<'a> {
    pub fn new() -> Self {
        Self {
            id: ContextId(Uuid::new_v4()),
            input_pipes: Vec::new(),
            feedback_pipes: IndexList::new(),
        }
    }
    pub fn input<T: 'a>(&mut self) -> (Input<T>, Relation<T, InputOp<T>>) {
        let (sender1, receiver1) = channel::new::<(T, isize)>();
        let (sender2, receiver2) = channel::new::<(T, ValueCount)>();
        let input_pipes = InputPipe {
            receiver: receiver1,
            sender: sender2,
        };
        self.input_pipes.push(Box::new(input_pipes));
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
        self.feedback_pipes.insert_last(Box::new(FeedbackPipe {
            relation,
            total: E1Map::new(),
            input,
        }));
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
    input_pipes: Vec<Box<dyn InputPipeT + 'a>>,
    feedback_pipes: IndexList<Box<dyn FeedbackPipeT + 'a>>,
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
struct Dropped;

struct InputPipe<T> {
    receiver: channel::Receiver<(T, isize)>,
    sender: channel::Sender<(T, ValueCount)>,
}

trait InputPipeT {
    fn process(&mut self, commit_id: CommitId) -> Result<(), Dropped>;
}

impl<T> InputPipeT for InputPipe<T> {
    fn process(&mut self, commit_id: CommitId) -> Result<(), Dropped> {
        while let Some((value, count)) = self.receiver.try_recv() {
            let value_count = ValueCount { commit_id, count };
            if self.sender.send((value, value_count)).is_err() {
                return Err(Dropped);
            }
        }
        Ok(())
    }
}

struct FeedbackPipe<T, C> {
    relation: Relation<T, C>,
    total: E1Map<T, ValueCount>,
    input: Input<T>,
}

trait FeedbackPipeT {
    fn process(&mut self, commit_id: CommitId) -> Result<bool, Dropped>;
}

impl<T: Eq + Hash + Clone, C: Op<T>> FeedbackPipeT for FeedbackPipe<T, C> {
    fn process(&mut self, commit_id: CommitId) -> Result<bool, Dropped> {
        self.relation.foreach(commit_id, |elem, count| {
            self.total.add(elem, count);
        });
        for (k, v) in self.total.iter() {
            if self.input.send_with_count(k.clone(), v.count).is_err() {
                return Err(Dropped);
            }
        }
        Ok(!self.total.is_empty())
    }
}

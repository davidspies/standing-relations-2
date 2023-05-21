use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use derivative::Derivative;

use crate::{
    channel,
    context::{CommitId, Dropped, Level},
    generic_map::AddMap,
    value_count::ValueCount,
    who::Who,
};

use super::{values::Values, PipeT, ProcessResult, Processable};

pub(crate) struct TrackedInputPipe<T> {
    receiver: channel::Receiver<(T, Who)>,
    sender: channel::Sender<(T, Level, ValueCount)>,
    received: Values<T>,
    frame_changes: Vec<Frame<T>>,
    changed_keys_scratch: HashSet<T>,
}

impl<T> TrackedInputPipe<T> {
    pub(crate) fn new(
        receiver: channel::Receiver<(T, Who)>,
        sender: channel::Sender<(T, Level, ValueCount)>,
    ) -> Self {
        TrackedInputPipe {
            receiver,
            sender,
            received: Values::default(),
            frame_changes: Vec::new(),
            changed_keys_scratch: HashSet::new(),
        }
    }

    fn level(&self) -> Level {
        Level(self.frame_changes.len())
    }
}

impl<T: Eq + Hash + Clone> Processable for TrackedInputPipe<T> {
    fn process(&mut self, _commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        let level = self.level();
        let mut result = ProcessResult::Unchanged;
        while let Some((value, who)) = self.receiver.try_recv() {
            self.changed_keys_scratch.insert(value.clone());
            if matches!(who, Who::User) {
                if let Some(frame) = self.frame_changes.last_mut() {
                    frame.user_values.add((value.clone(), ValueCount(1)));
                }
            }
            self.received.values.add((value, who.value_count()));
        }
        for value in self.changed_keys_scratch.drain() {
            if self.received.values.contains_key(&value) {
                if self.received.seen.insert(value.clone()) {
                    result = ProcessResult::Changed;
                    if let Some(frame) = self.frame_changes.last_mut() {
                        frame.seen.insert(value.clone());
                    }
                    if self.sender.send((value, level, ValueCount(1))).is_err() {
                        return Err(Dropped);
                    }
                }
            }
        }
        Ok(result)
    }
}

impl<T: Eq + Hash + Clone> PipeT for TrackedInputPipe<T> {
    fn push_frame(&mut self) {
        self.frame_changes.push(Frame::default());
    }
    fn pop_frame(&mut self) -> Result<(), Dropped> {
        let level = self.level();
        let frame = self.frame_changes.pop().unwrap();
        for (value, count) in frame.user_values {
            self.received.values.add((value, -count));
        }
        for value in frame.seen {
            self.received.seen.remove(&value);
            if self.sender.send((value, level, ValueCount(-1))).is_err() {
                return Err(Dropped);
            }
        }
        Ok(())
    }
}

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
struct Frame<T> {
    user_values: HashMap<T, ValueCount>,
    seen: HashSet<T>,
}

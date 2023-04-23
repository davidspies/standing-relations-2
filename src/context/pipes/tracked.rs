use std::{
    cmp::Reverse,
    collections::{hash_map, HashMap, HashSet},
    hash::Hash,
};

use derivative::Derivative;

use crate::{
    channel,
    context::{CommitId, DataId, Dropped, Ids},
    generic_map::AddMap,
    value_count::ValueCount,
    who::Who,
};

use super::{values::Values, PipeT, ProcessResult, Processable};

pub(crate) struct TrackedInputPipe<T> {
    receiver: channel::Receiver<(T, Who)>,
    sender: channel::Sender<(T, Ids, ValueCount)>,
    received: Values<T>,
    frame_changes: Vec<Frame<T>>,
    changed_keys_scratch: HashSet<T>,
}

impl<T> TrackedInputPipe<T> {
    pub(crate) fn new(
        receiver: channel::Receiver<(T, Who)>,
        sender: channel::Sender<(T, Ids, ValueCount)>,
    ) -> Self {
        TrackedInputPipe {
            receiver,
            sender,
            received: Values::default(),
            frame_changes: Vec::new(),
            changed_keys_scratch: HashSet::new(),
        }
    }
}

impl<T: Eq + Hash + Clone> Processable for TrackedInputPipe<T> {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        let ids: Ids = Ids::processed(commit_id);
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
                if let hash_map::Entry::Vacant(vac) = self.received.seen.entry(value.clone()) {
                    result = ProcessResult::Changed;
                    vac.insert(ids.data_id());
                    if let Some(frame) = self.frame_changes.last_mut() {
                        frame.seen.insert(value.clone(), ids.data_id());
                    }
                    if self.sender.send((value, ids, ValueCount(1))).is_err() {
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
    fn pop_frame(&mut self, commit_id: CommitId) -> Result<(), Dropped> {
        let frame = self.frame_changes.pop().unwrap();
        for (value, count) in frame.user_values {
            self.received.values.add((value, -count));
        }
        let mut to_send = Vec::from_iter(frame.seen);
        to_send.sort_by_key(|&(_, data_id)| Reverse(data_id));
        for (value, data_id) in to_send {
            self.received.seen.remove(&value);
            if self
                .sender
                .send((value, Ids::new(commit_id, data_id), ValueCount(-1)))
                .is_err()
            {
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
    seen: HashMap<T, DataId>,
}

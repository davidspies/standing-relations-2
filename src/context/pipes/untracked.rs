use std::{
    collections::{hash_map, HashSet},
    hash::Hash,
};

use crate::{
    channel,
    context::{CommitId, Dropped, Ids},
    generic_map::AddMap,
    value_count::ValueCount,
    who::Who,
};

use super::{values::Values, PipeT, ProcessResult, Processable};

pub(crate) struct UntrackedInputPipe<T> {
    receiver: channel::Receiver<(T, Who)>,
    sender: channel::Sender<(T, Ids, ValueCount)>,
    received: Values<T>,
    changed_keys_scratch: HashSet<T>,
}

impl<T> UntrackedInputPipe<T> {
    pub(crate) fn new(
        receiver: channel::Receiver<(T, Who)>,
        sender: channel::Sender<(T, Ids, ValueCount)>,
    ) -> Self {
        UntrackedInputPipe {
            receiver,
            sender,
            received: Values::default(),
            changed_keys_scratch: HashSet::new(),
        }
    }
}

impl<T: Eq + Hash + Clone> Processable for UntrackedInputPipe<T> {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        let ids: Ids = Ids::processed(commit_id);
        let mut result = ProcessResult::Unchanged;
        while let Some((value, who)) = self.receiver.try_recv() {
            self.changed_keys_scratch.insert(value.clone());
            self.received.values.add((value, who.value_count()));
        }
        for value in self.changed_keys_scratch.drain() {
            if self.received.values.contains_key(&value) {
                if let hash_map::Entry::Vacant(vac) = self.received.seen.entry(value.clone()) {
                    result = ProcessResult::Changed;
                    vac.insert(ids.data_id());
                    if self.sender.send((value, ids, ValueCount(1))).is_err() {
                        return Err(Dropped);
                    }
                }
            }
        }
        Ok(result)
    }
}

impl<T: Eq + Hash + Clone> PipeT for UntrackedInputPipe<T> {
    fn push_frame(&mut self) {}
    fn pop_frame(&mut self, _commit_id: CommitId) -> Result<(), Dropped> {
        Ok(())
    }
}

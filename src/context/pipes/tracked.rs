use std::{cmp::Reverse, collections::HashMap, hash::Hash};

use crate::{
    channel,
    context::{CommitId, DataId, Dropped, Ids},
    generic_map::AddMap,
    value_count::ValueCount,
};

use super::{PipeT, ProcessResult};

pub(crate) struct TrackedInputPipe<T> {
    receiver: channel::Receiver<(T, ValueCount)>,
    sender: channel::Sender<(T, Ids, ValueCount)>,
    frame_changes: Vec<HashMap<(T, DataId), ValueCount>>,
}
impl<T> TrackedInputPipe<T> {
    pub(crate) fn new(
        receiver: channel::Receiver<(T, ValueCount)>,
        sender: channel::Sender<(T, Ids, ValueCount)>,
    ) -> Self {
        TrackedInputPipe {
            receiver,
            sender,
            frame_changes: Vec::new(),
        }
    }
}

impl<T: Eq + Hash + Clone> PipeT for TrackedInputPipe<T> {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        let ids: Ids = Ids::processed(commit_id);
        let mut result = ProcessResult::Unchanged;
        while let Some((value, count)) = self.receiver.try_recv() {
            result = ProcessResult::Changed;
            if let Some(frame) = self.frame_changes.last_mut() {
                frame.add(((value.clone(), ids.data_id()), count));
            }
            if self.sender.send((value, ids, count)).is_err() {
                return Err(Dropped);
            }
        }
        Ok(result)
    }
    fn push_frame(&mut self) {
        self.frame_changes.push(HashMap::new());
    }
    fn pop_frame(&mut self, commit_id: CommitId) -> Result<(), Dropped> {
        let frame = self.frame_changes.pop().unwrap();
        let mut to_send = Vec::from_iter(
            frame
                .into_iter()
                .map(|((value, data_id), value_count)| (value, data_id, value_count)),
        );
        to_send.sort_by_key(|&(_, data_id, _)| Reverse(data_id));
        for (value, data_id, value_count) in to_send {
            if self
                .sender
                .send((value, Ids::new(commit_id, data_id), -value_count))
                .is_err()
            {
                return Err(Dropped);
            }
        }
        Ok(())
    }
}

use std::hash::Hash;

use crate::{
    channel,
    context::{CommitId, Dropped},
    rollover_map::RolloverMap,
    value_count::ValueCount,
};

use super::{PipeT, ProcessResult};

pub(crate) struct TrackedInputPipe<T> {
    receiver: channel::Receiver<(T, isize)>,
    sender: channel::Sender<(T, ValueCount)>,
    frame_changes: Vec<RolloverMap<T, ValueCount>>,
}
impl<T> TrackedInputPipe<T> {
    pub(crate) fn new(
        receiver: channel::Receiver<(T, isize)>,
        sender: channel::Sender<(T, ValueCount)>,
    ) -> Self {
        TrackedInputPipe {
            receiver,
            sender,
            frame_changes: Vec::new(),
        }
    }
}

impl<T: Eq + Hash + Clone> PipeT for TrackedInputPipe<T> {
    fn process(&mut self, _commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        let mut result = ProcessResult::Unchanged;
        while let Some((value, count)) = self.receiver.try_recv() {
            result = ProcessResult::Changed;
            if let Some(frame) = self.frame_changes.last_mut() {
                frame.add(value.clone(), count);
            }
            if self.sender.send((value, count)).is_err() {
                return Err(Dropped);
            }
        }
        Ok(result)
    }
    fn push_frame(&mut self) {
        self.frame_changes.push(RolloverMap::new());
    }
    fn pop_frame(&mut self) -> Result<(), Dropped> {
        let frame = self.frame_changes.pop().unwrap();
        for (value, value_count) in frame {
            if self.sender.send((value, -value_count)).is_err() {
                return Err(Dropped);
            }
        }
        Ok(())
    }
}

use std::hash::Hash;

use crate::{
    channel,
    context::{CommitId, Dropped},
    e1map::E1Map,
    value_count::ValueCount,
};

use super::PipeT;

pub(crate) struct TrackedInputPipe<T> {
    receiver: channel::Receiver<(T, isize)>,
    sender: channel::Sender<(T, ValueCount)>,
    frame_changes: Vec<E1Map<T, ValueCount>>,
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
    fn process(&mut self, _commit_id: CommitId) -> Result<bool, Dropped> {
        let mut any_changed = false;
        while let Some((value, count)) = self.receiver.try_recv() {
            any_changed = true;
            if let Some(frame) = self.frame_changes.last_mut() {
                frame.add(value.clone(), count);
            }
            if self.sender.send((value, count)).is_err() {
                return Err(Dropped);
            }
        }
        Ok(any_changed)
    }
    fn push_frame(&mut self) {
        self.frame_changes.push(E1Map::new());
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

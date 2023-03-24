use crate::{
    channel,
    context::{CommitId, Dropped},
    value_count::ValueCount,
};

use super::PipeT;

pub(crate) struct UntrackedInputPipe<T> {
    receiver: channel::Receiver<(T, isize)>,
    sender: channel::Sender<(T, ValueCount)>,
}
impl<T> UntrackedInputPipe<T> {
    pub(crate) fn new(
        receiver: channel::Receiver<(T, isize)>,
        sender: channel::Sender<(T, ValueCount)>,
    ) -> Self {
        UntrackedInputPipe { receiver, sender }
    }
}

impl<T> PipeT for UntrackedInputPipe<T> {
    fn process(&mut self, commit_id: CommitId) -> Result<bool, Dropped> {
        let mut any_changed = false;
        while let Some((value, count)) = self.receiver.try_recv() {
            any_changed = true;
            let value_count = ValueCount { commit_id, count };
            if self.sender.send((value, value_count)).is_err() {
                return Err(Dropped);
            }
        }
        Ok(any_changed)
    }
    fn push_frame(&mut self) {}
    fn pop_frame(&mut self) -> Result<(), Dropped> {
        Ok(())
    }
}

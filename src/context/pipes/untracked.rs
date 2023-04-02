use crate::{
    channel,
    context::{CommitId, Dropped},
    value_count::ValueCount,
};

use super::{PipeT, ProcessResult};

pub(crate) struct UntrackedInputPipe<T> {
    receiver: channel::Receiver<(T, ValueCount)>,
    sender: channel::Sender<(T, ValueCount)>,
}
impl<T> UntrackedInputPipe<T> {
    pub(crate) fn new(
        receiver: channel::Receiver<(T, ValueCount)>,
        sender: channel::Sender<(T, ValueCount)>,
    ) -> Self {
        UntrackedInputPipe { receiver, sender }
    }
}

impl<T> PipeT for UntrackedInputPipe<T> {
    fn process(&mut self, _commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        let mut result = ProcessResult::Unchanged;
        while let Some((value, count)) = self.receiver.try_recv() {
            result = ProcessResult::Changed;
            if self.sender.send((value, count)).is_err() {
                return Err(Dropped);
            }
        }
        Ok(result)
    }
    fn push_frame(&mut self) {}
    fn pop_frame(&mut self, _commit_id: CommitId) -> Result<(), Dropped> {
        Ok(())
    }
}

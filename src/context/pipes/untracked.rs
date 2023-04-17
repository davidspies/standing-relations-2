use crate::{
    channel,
    context::{CommitId, Dropped, Ids},
    value_count::ValueCount,
};

use super::{PipeT, ProcessResult};

pub(crate) struct UntrackedInputPipe<T> {
    receiver: channel::Receiver<(T, ValueCount)>,
    sender: channel::Sender<(T, Ids, ValueCount)>,
}
impl<T> UntrackedInputPipe<T> {
    pub(crate) fn new(
        receiver: channel::Receiver<(T, ValueCount)>,
        sender: channel::Sender<(T, Ids, ValueCount)>,
    ) -> Self {
        UntrackedInputPipe { receiver, sender }
    }
}

impl<T> PipeT for UntrackedInputPipe<T> {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped> {
        let mut result = ProcessResult::Unchanged;
        let ids = Ids::processed(commit_id);
        while let Some((value, count)) = self.receiver.try_recv() {
            result = ProcessResult::Changed;
            if self.sender.send((value, ids, count)).is_err() {
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

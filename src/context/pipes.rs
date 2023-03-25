use self::interrupt::InterruptId;

use super::{CommitId, Dropped};

pub(crate) mod feedback;
pub(crate) mod interrupt;
pub(crate) mod tracked;
pub(crate) mod untracked;

pub(crate) trait PipeT {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped>;
    fn push_frame(&mut self);
    fn pop_frame(&mut self) -> Result<(), Dropped>;
}

pub(crate) enum ProcessResult {
    Changed,
    Unchanged,
    Interrupted(InterruptId),
}

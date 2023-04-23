use self::interrupt::InterruptId;

use super::{CommitId, Dropped};

pub(crate) mod feedback;
pub(crate) mod interrupt;
#[cfg(feature = "redis")]
pub(crate) mod redis;
pub(crate) mod tracked;
pub(crate) mod untracked;
pub(crate) mod values;

pub(crate) trait Processable {
    fn process(&mut self, commit_id: CommitId) -> Result<ProcessResult, Dropped>;
}

pub(crate) trait PipeT: Processable {
    fn push_frame(&mut self);
    fn pop_frame(&mut self, commit_id: CommitId) -> Result<(), Dropped>;
}

pub(crate) enum ProcessResult {
    Changed,
    Unchanged,
    Interrupted(InterruptId),
}

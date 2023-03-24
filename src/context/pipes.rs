use super::{CommitId, Dropped};

pub(crate) mod feedback;
pub(crate) mod tracked;
pub(crate) mod untracked;

pub(crate) trait PipeT {
    fn process(&mut self, commit_id: CommitId) -> Result<bool, Dropped>;
    fn push_frame(&mut self);
    fn pop_frame(&mut self) -> Result<(), Dropped>;
}

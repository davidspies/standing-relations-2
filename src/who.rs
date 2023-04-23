use crate::ValueCount;

pub(crate) enum Who {
    User,
    Feedback(ValueCount),
}

impl Who {
    pub(crate) fn value_count(&self) -> ValueCount {
        match self {
            Who::User => ValueCount(1),
            Who::Feedback(count) => *count,
        }
    }
}

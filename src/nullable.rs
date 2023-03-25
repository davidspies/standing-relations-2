pub(crate) trait Nullable: Default {
    fn is_empty(&self) -> bool;
}

impl Nullable for isize {
    fn is_empty(&self) -> bool {
        *self == 0
    }
}

pub(crate) trait Nullable: Default {
    fn is_empty(&self) -> bool;
}

pub(crate) trait AddToValue<V> {
    #[must_use]
    fn add_to(self, v: &mut V) -> ValueChanges;
}

pub(crate) struct ValueChanges {
    pub(crate) was_zero: bool,
    pub(crate) is_zero: bool,
}

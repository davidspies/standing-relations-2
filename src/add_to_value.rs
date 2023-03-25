pub(crate) trait AddToValue<V> {
    #[must_use]
    fn add_to(self, v: &mut V) -> ValueChanges;
}

pub(crate) struct ValueChanges {
    pub(crate) was_zero: bool,
    pub(crate) is_zero: bool,
}

impl AddToValue<isize> for isize {
    fn add_to(self, v: &mut isize) -> ValueChanges {
        let was_zero = *v == 0;
        *v += self;
        let is_zero = *v == 0;
        ValueChanges { was_zero, is_zero }
    }
}

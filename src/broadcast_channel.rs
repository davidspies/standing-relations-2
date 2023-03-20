use std::cell::RefCell;

use derivative::Derivative;

use crate::channel;

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
pub struct Sender<T>(RefCell<Vec<channel::Sender<T>>>);

pub type Receiver<T> = channel::Receiver<T>;

impl<T> Sender<T> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn subscribe(&mut self) -> Receiver<T> {
        let (sender, receiver) = channel::new();
        self.0.borrow_mut().push(sender);
        receiver
    }
    pub fn send(&mut self, value: T)
    where
        T: Clone,
    {
        self.0
            .get_mut()
            .retain_mut(|sender| sender.send(value.clone()).is_ok())
    }
}

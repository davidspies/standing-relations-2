use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::{Rc, Weak},
};

use derivative::Derivative;

#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct Sender<T>(Weak<RefCell<VecDeque<T>>>);

pub struct Receiver<T>(Rc<RefCell<VecDeque<T>>>);

pub fn new<T>() -> (Sender<T>, Receiver<T>) {
    let queue = Rc::new(RefCell::new(VecDeque::new()));
    (Sender(Rc::downgrade(&queue)), Receiver(queue))
}

impl<T> Sender<T> {
    pub fn send(&mut self, value: T) -> Result<(), T> {
        match self.0.upgrade() {
            Some(this) => {
                this.borrow_mut().push_back(value);
                Ok(())
            }
            None => Err(value),
        }
    }
}

impl<T> Receiver<T> {
    pub fn try_recv(&mut self) -> Option<T> {
        self.0.borrow_mut().pop_front()
    }
}

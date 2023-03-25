use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::{Rc, Weak},
    vec,
};

use derivative::Derivative;

#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct Sender<T>(Weak<RefCell<VecDeque<T>>>);

pub struct Receiver<T> {
    queue: Rc<RefCell<VecDeque<T>>>,
    iter_scratch: Vec<T>,
}

pub fn new<T>() -> (Sender<T>, Receiver<T>) {
    let queue = Rc::new(RefCell::new(VecDeque::new()));
    (
        Sender(Rc::downgrade(&queue)),
        Receiver {
            queue,
            iter_scratch: Vec::new(),
        },
    )
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
        self.queue.borrow_mut().pop_front()
    }
    pub fn try_iter(&mut self) -> vec::Drain<T> {
        self.iter_scratch.extend(self.queue.borrow_mut().drain(..));
        self.iter_scratch.drain(..)
    }
}

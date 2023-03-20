use derivative::Derivative;
use parking_lot::Mutex;

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
pub struct Sender<T>(Mutex<Vec<crossbeam_channel::Sender<T>>>);

pub struct Receiver<T>(crossbeam_channel::Receiver<T>);

impl<T> Sender<T> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn receiver(&mut self) -> Receiver<T> {
        let (sender, receiver) = crossbeam_channel::unbounded();
        self.0.lock().push(sender);
        Receiver(receiver)
    }
    pub fn send(&mut self, value: T)
    where
        T: Clone,
    {
        self.0
            .get_mut()
            .retain(|sender| sender.send(value.clone()).is_ok())
    }
}

impl<T> Receiver<T> {
    pub fn try_recv(&mut self) -> Option<T> {
        self.0.try_recv().ok()
    }

    pub fn try_iter(&mut self) -> crossbeam_channel::TryIter<T> {
        self.0.try_iter()
    }
}

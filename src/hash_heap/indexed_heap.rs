use derivative::Derivative;

#[derive(Derivative)]
#[derivative(Default(bound = "C: Default"))]
pub(super) struct IndexedHeap<T, C> {
    comparator: C,
    values: Vec<T>,
}

impl<T, C> IndexedHeap<T, C> {
    pub(super) fn peek(&self) -> Option<&T> {
        self.values.get(0)
    }

    pub(crate) fn clear(&mut self) {
        self.values.clear();
    }
}

impl<T: Clone + Ord, C: Comparator> IndexedHeap<T, C> {
    pub(super) fn insert(
        &mut self,
        val: T,
        changed_indices_scratch: &mut Vec<(T, usize)>,
    ) -> usize {
        let mut new_index = self.values.len();
        self.values.push(val);
        while new_index != 0 {
            let parent_index = parent(new_index);
            if !self
                .comparator
                .favors(&self.values[new_index], &self.values[parent_index])
            {
                break;
            }
            changed_indices_scratch.push((self.values[parent_index].clone(), new_index));
            self.values.swap(parent_index, new_index);
            new_index = parent_index;
        }
        new_index
    }
    pub(super) fn remove(&mut self, index: usize, changed_indices_scratch: &mut Vec<(T, usize)>) {
        let last_index = self.values.len() - 1;
        self.values.swap(index, last_index);
        self.values.pop();
        if index == last_index {
            return;
        }
        let mut current_index = index;
        loop {
            let (left_child_index, right_child_index) = children(current_index);
            let mut favored_child_index = left_child_index;
            if right_child_index < self.values.len() {
                if self.comparator.favors(
                    &self.values[right_child_index],
                    &self.values[left_child_index],
                ) {
                    favored_child_index = right_child_index;
                }
            }
            if favored_child_index >= self.values.len()
                || !self.comparator.favors(
                    &self.values[favored_child_index],
                    &self.values[current_index],
                )
            {
                break;
            }
            changed_indices_scratch.push((self.values[favored_child_index].clone(), current_index));
            self.values.swap(current_index, favored_child_index);
            current_index = favored_child_index;
        }
        changed_indices_scratch.push((self.values[current_index].clone(), current_index));
    }
}

fn parent(i: usize) -> usize {
    (i - 1) / 2
}

fn children(i: usize) -> (usize, usize) {
    (2 * i + 1, 2 * i + 2)
}

pub trait Comparator {
    fn favors<T: Ord>(&self, lhs: &T, rhs: &T) -> bool;
}

#[derive(Default)]
pub struct Min;

impl Comparator for Min {
    fn favors<T: Ord>(&self, lhs: &T, rhs: &T) -> bool {
        lhs < rhs
    }
}

#[derive(Default)]
pub struct Max;

impl Comparator for Max {
    fn favors<T: Ord>(&self, lhs: &T, rhs: &T) -> bool {
        lhs > rhs
    }
}

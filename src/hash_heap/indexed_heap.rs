use std::ops::Index;

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

impl<T, C: Comparator<T>> IndexedHeap<T, C> {
    pub(super) fn insert(&mut self, val: T, changed_indices_scratch: &mut Vec<usize>) -> usize {
        let mut new_index = self.values.len();
        self.values.push(val);
        while let Some(parent_index) = parent(new_index) {
            if !self
                .comparator
                .favors(&self.values[new_index], &self.values[parent_index])
            {
                break;
            }
            changed_indices_scratch.push(new_index);
            self.values.swap(parent_index, new_index);
            new_index = parent_index;
        }
        new_index
    }
    pub(super) fn remove(&mut self, index: usize, changed_indices_scratch: &mut Vec<usize>) {
        let last_index = self.values.len() - 1;
        self.values.swap(index, last_index);
        self.values.pop();
        if index == last_index {
            return;
        }
        let mut current_index = index;
        while let Some(parent_index) = parent(current_index) {
            if current_index == 0
                || !self
                    .comparator
                    .favors(&self.values[current_index], &self.values[parent_index])
            {
                break;
            }
            changed_indices_scratch.push(current_index);
            self.values.swap(parent_index, current_index);
            current_index = parent_index;
        }
        loop {
            let (left_child_index, right_child_index) = children(current_index);
            let favored_child_index = if right_child_index < self.values.len()
                && self.comparator.favors(
                    &self.values[right_child_index],
                    &self.values[left_child_index],
                ) {
                right_child_index
            } else {
                left_child_index
            };
            changed_indices_scratch.push(current_index);
            if favored_child_index >= self.values.len()
                || !self.comparator.favors(
                    &self.values[favored_child_index],
                    &self.values[current_index],
                )
            {
                break;
            }
            self.values.swap(current_index, favored_child_index);
            current_index = favored_child_index;
        }
    }
}

impl<T, C> Index<usize> for IndexedHeap<T, C> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

fn parent(i: usize) -> Option<usize> {
    if i == 0 {
        None
    } else {
        Some((i - 1) / 2)
    }
}

fn children(i: usize) -> (usize, usize) {
    (2 * i + 1, 2 * i + 2)
}

pub trait Comparator<T> {
    fn favors(&self, lhs: &T, rhs: &T) -> bool;
}

#[derive(Default)]
pub struct Min;

impl<T: PartialOrd> Comparator<T> for Min {
    fn favors(&self, lhs: &T, rhs: &T) -> bool {
        lhs < rhs
    }
}

#[derive(Default)]
pub struct Max;

impl<T: PartialOrd> Comparator<T> for Max {
    fn favors(&self, lhs: &T, rhs: &T) -> bool {
        lhs > rhs
    }
}

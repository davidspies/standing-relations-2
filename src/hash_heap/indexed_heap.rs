use derivative::Derivative;

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
pub(super) struct IndexedHeap<T>(Vec<T>);

impl<T> IndexedHeap<T> {
    pub(super) fn peek(&self) -> Option<&T> {
        self.0.get(0)
    }

    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }
}

impl<T: Copy + Ord> IndexedHeap<T> {
    pub(super) fn insert(
        &mut self,
        val: T,
        changed_indices_scratch: &mut Vec<(T, usize)>,
    ) -> usize {
        let mut new_index = self.0.len();
        self.0.push(val);
        while new_index != 0 {
            let parent_index = parent(new_index);
            if self.0[parent_index] >= self.0[new_index] {
                break;
            }
            changed_indices_scratch.push((self.0[parent_index], new_index));
            self.0.swap(parent_index, new_index);
            new_index = parent_index;
        }
        new_index
    }
    pub(super) fn remove(&mut self, index: usize, changed_indices_scratch: &mut Vec<(T, usize)>) {
        let last_index = self.0.len() - 1;
        self.0.swap(index, last_index);
        self.0.pop();
        if index == last_index {
            return;
        }
        let mut current_index = index;
        loop {
            let (left_child_index, right_child_index) = children(current_index);
            let mut max_child_index = left_child_index;
            if right_child_index < self.0.len() {
                if self.0[right_child_index] > self.0[left_child_index] {
                    max_child_index = right_child_index;
                }
            }
            if max_child_index >= self.0.len() || self.0[current_index] >= self.0[max_child_index] {
                break;
            }
            changed_indices_scratch.push((self.0[max_child_index], current_index));
            self.0.swap(current_index, max_child_index);
            current_index = max_child_index;
        }
        changed_indices_scratch.push((self.0[current_index], current_index));
    }
}

fn parent(i: usize) -> usize {
    (i - 1) / 2
}

fn children(i: usize) -> (usize, usize) {
    (2 * i + 1, 2 * i + 2)
}

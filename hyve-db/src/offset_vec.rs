use std::ops::{Index, IndexMut, Range, RangeFrom, RangeFull, RangeTo};

use derive_where::derive_where;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive_where(Default)]
#[serde(transparent)]
pub struct OffsetVec<T>(Vec<T>);

impl<T> OffsetVec<T> {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn push(&mut self, value: T) {
        self.0.push(value);
    }

    pub fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }

    pub fn last(&self) -> Option<&T> {
        self.0.last()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> + ExactSizeIterator {
        self.0.iter()
    }
}

impl<T> Index<usize> for OffsetVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        assert!(index > 0, "Index out of bounds");
        &self.0[index - 1]
    }
}

impl<T> IndexMut<usize> for OffsetVec<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        assert!(index > 0, "Index out of bounds");
        &mut self.0[index - 1]
    }
}

impl<T> Index<Range<usize>> for OffsetVec<T> {
    type Output = [T];

    fn index(&self, range: Range<usize>) -> &Self::Output {
        assert!(range.start > 0 && range.end > 0, "Index out of bounds");
        &self.0[range.start - 1..range.end - 1]
    }
}

impl<T> IndexMut<Range<usize>> for OffsetVec<T> {
    fn index_mut(&mut self, range: Range<usize>) -> &mut Self::Output {
        assert!(range.start > 0 && range.end > 0, "Index out of bounds");
        &mut self.0[range.start - 1..range.end - 1]
    }
}

impl<T> Index<RangeFrom<usize>> for OffsetVec<T> {
    type Output = [T];

    fn index(&self, range: RangeFrom<usize>) -> &Self::Output {
        assert!(range.start > 0, "Index out of bounds");
        &self.0[range.start - 1..]
    }
}

impl<T> IndexMut<RangeFrom<usize>> for OffsetVec<T> {
    fn index_mut(&mut self, range: RangeFrom<usize>) -> &mut Self::Output {
        assert!(range.start > 0, "Index out of bounds");
        &mut self.0[range.start - 1..]
    }
}

impl<T> Index<RangeTo<usize>> for OffsetVec<T> {
    type Output = [T];

    fn index(&self, range: RangeTo<usize>) -> &Self::Output {
        assert!(range.end > 0, "Index out of bounds");
        &self.0[..range.end - 1]
    }
}

impl<T> IndexMut<RangeTo<usize>> for OffsetVec<T> {
    fn index_mut(&mut self, range: RangeTo<usize>) -> &mut Self::Output {
        assert!(range.end > 0, "Index out of bounds");
        &mut self.0[..range.end - 1]
    }
}

impl<T> Index<RangeFull> for OffsetVec<T> {
    type Output = [T];

    fn index(&self, range: RangeFull) -> &Self::Output {
        &self.0[range]
    }
}

impl<T> Extend<T> for OffsetVec<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.0.extend(iter);
    }
}

impl<T> From<Vec<T>> for OffsetVec<T> {
    fn from(vec: Vec<T>) -> Self {
        Self(vec)
    }
}

impl<T> From<OffsetVec<T>> for Vec<T> {
    fn from(offset_vec: OffsetVec<T>) -> Self {
        offset_vec.0
    }
}

impl<T> FromIterator<T> for OffsetVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self(Vec::from_iter(iter))
    }
}

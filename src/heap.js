// A binary min-heap. push/pop are O(log n), peek is O(1).
// `compare(a, b)` returns a negative number when a should come out first.
class MinHeap {
  constructor(compare) {
    this.compare = compare;
    this.items = [];
  }

  get size() {
    return this.items.length;
  }

  peek() {
    return this.items[0];
  }

  push(item) {
    const items = this.items;
    items.push(item);
    let i = items.length - 1;
    // Bubble up until the parent is smaller
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this.compare(items[i], items[parent]) >= 0) break;
      [items[i], items[parent]] = [items[parent], items[i]];
      i = parent;
    }
  }

  pop() {
    const items = this.items;
    if (items.length === 0) return undefined;
    const top = items[0];
    const last = items.pop();
    if (items.length > 0) {
      items[0] = last;
      // Sink down until both children are larger
      let i = 0;
      for (;;) {
        const left = 2 * i + 1;
        const right = left + 1;
        let smallest = i;
        if (left < items.length && this.compare(items[left], items[smallest]) < 0) smallest = left;
        if (right < items.length && this.compare(items[right], items[smallest]) < 0) smallest = right;
        if (smallest === i) break;
        [items[i], items[smallest]] = [items[smallest], items[i]];
        i = smallest;
      }
    }
    return top;
  }

  // Remove every item matching `predicate`. O(n); used only for rare admin actions.
  removeWhere(predicate) {
    const kept = this.items.filter((x) => !predicate(x));
    const removed = this.items.length - kept.length;
    this.items = [];
    kept.forEach((x) => this.push(x));
    return removed;
  }
}

module.exports = { MinHeap };

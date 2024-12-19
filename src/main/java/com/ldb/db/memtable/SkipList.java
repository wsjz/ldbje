package com.ldb.db.memtable;

import com.ldb.utils.SeekableIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class SkipList<KEY extends Comparable<? super KEY>> {
    private final Node<KEY> head;
    private final Random rnd = ThreadLocalRandom.current();
    private final Comparator<? super KEY> comparator;
    private volatile int maxHeight; // 保证修改后，其他线程立即可见
    private static final int kBranching = 4;
    private static final int kMaxHeight = 12;

    public SkipList() {
        this.head = newNode(null, kMaxHeight);
        this.comparator = new DefaultComparator<>();
        this.maxHeight = 1;
    }

    public SkipList(Comparator<? super KEY> comparator) {
        this.head = newNode(null, kMaxHeight);
        this.comparator = comparator;
        this.maxHeight = 1;
    }

    public void insert(KEY key) {
        List<Node<KEY>> prev = new ArrayList<>(Collections.nCopies(kMaxHeight, null));
        Node<KEY> x = findGreaterOrEqual(key, prev);
        if (x != null && equal(key, x.key)) {
            throw new IllegalArgumentException("Key " + key + " already exists");
        }
        int height = randomHeight();
        if (height > maxHeight) {
            for (int i = maxHeight; i < height; i++) {
                prev.set(i, head);
            }
            maxHeight = height;
        }
        x = newNode(key, height);
        for (int i = 0; i < height; i++) {
            x.unprotectedSetNext(i, prev.get(i).unprotectedNext(i));
            prev.get(i).setNext(i, x);
        }
    }

    private int randomHeight() {
        int height = 1;
        // 位运算代替取模运算rnd % kBranching，等于0的概率是1/kBranching，这时增加高度
        while (height < kMaxHeight && (rnd.nextInt() & (kBranching - 1)) == 0) {
            height++;
        }
        return height;
    }

    private Node<KEY> newNode(KEY key, int height) {
        // TODO：使用对象池/内存池分配节点
        return new Node<>(key, height);
    }

    public boolean contains(KEY key) {
        Node<KEY> x = findGreaterOrEqual(key);
        // TODO: 什么时候x为null
        return x != null && equal(key, x.key);
    }

    private boolean equal(KEY key1, KEY key2) {
        return comparator.compare(key1, key2) == 0;
    }

    private boolean greaterOrEqual(KEY key1, KEY key2) {
        return comparator.compare(key1, key2) >= 0;
    }

    private Node<KEY> findGreaterOrEqual(KEY key) {
        return findGreaterOrEqual(key, null);
    }

    private Node<KEY> findGreaterOrEqual(KEY key,List<Node<KEY>> prev) {
        Node<KEY> x = head;
        int level = maxHeight - 1;
        while (true) {
            Node<KEY> next = x.next(level);
            if (next == null || greaterOrEqual(next.key, key)) {
                if (prev != null) {
                    prev.set(level, x);
                }
                if (level == 0) {
                    return next;
                }
                level--;
            } else {
                x = next;
            }
        }
    }

    private Node<KEY> findLessThan(KEY key) {
        Node<KEY> x = head;
        int level = maxHeight - 1;
        while (true) {
            Node<KEY> next = x.next(level);
            if (next == null || greaterOrEqual(next.key, key)) {
                if (level == 0) {
                    return x;
                }
                level--;
            } else {
                x = next;
            }
        }
    }

    private Node<KEY> findLast() {
        Node<KEY> x = head;
        int level = maxHeight - 1;
        while (true) {
            Node<KEY> next = x.next(level);
            if (next == null) {
                if (level == 0) {
                    return x;
                }
                level--;
            } else {
                x = next;
            }
        }
    }

    public SeekableIterator<KEY> iterator() {
        return new SeekableIterator<>() {
            private final SkipList<KEY> list = SkipList.this;
            private Node<KEY> node;

            @Override
            public boolean valid() {
                return node != null;
            }

            private void checkValid() {
                if (!valid()) {
                    throw new IllegalStateException("Key is not valid");
                }
            }

            @Override
            public KEY key() {
                checkValid();
                return node.key;
            }

            @Override
            public void next() {
                checkValid();
                node = node.next(0);
            }


            @Override
            public void prev() {
                checkValid();
                node = list.findLessThan(node.key);
                if (node == list.head) {
                    node = null;
                }
            }

            @Override
            public void seek(KEY target) {
                node = list.findGreaterOrEqual(target);
            }

            @Override
            public void seekToFirst() {
                node = list.head.next(0);
            }

            @Override
            public void seekToLast() {
                node = list.findLast();
                if (node == list.head) {
                    node = null;
                }
            }
        };
    }

    static class Node<KEY> {
        private final KEY key;
        // TODO: 变长数组更好一些，自定义VarHandle优化内存？
        private final AtomicReferenceArray<Node<KEY>> next;
        private Node(KEY key, int height) {
            this.key = key;
            this.next = new AtomicReferenceArray<>(height);
        }

        protected Node<KEY> next(int level) {
            if (level < 0) {
                throw new IllegalArgumentException();
            }
            return next.getAcquire(level);
        }

        protected void setNext(int level, Node<KEY> next) {
            if (level < 0) {
                throw new IllegalArgumentException();
            }
            this.next.setRelease(level, next);
        }

        protected Node<KEY> unprotectedNext(int level) {
            if (level < 0) {
                throw new IllegalArgumentException();
            }
            return next.getPlain(level);
        }

        protected void unprotectedSetNext(int level, Node<KEY> next) {
            if (level < 0) {
                throw new IllegalArgumentException();
            }
            this.next.setPlain(level, next);
        }
    }

    static class DefaultComparator<KEY extends Comparable<? super KEY>>
            implements Comparator<KEY> {
        @Override
        public int compare(KEY o1, KEY o2) {
            return o1.compareTo(o2);
        }
    }
}

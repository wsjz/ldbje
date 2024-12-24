package com.ldb.db.memtable;

import java.util.Comparator;

public class MemTable {
    private int ref;
    private final Comparator<InternalKey> comparator;

    public MemTable(Comparator<InternalKey> comparator) {
        this.comparator = comparator;
    }

    public long approximateMemoryUsage() {
        return 0L;
    }

    public void ref() {
        ref++;
    }
}

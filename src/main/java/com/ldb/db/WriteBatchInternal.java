package com.ldb.db;

import com.ldb.db.memtable.MemTable;

import java.nio.ByteBuffer;

public class WriteBatchInternal {
    public static void setSequence(WriteBatch writeBatch, long l) {
    }

    public static Status insertInto(WriteBatch writeBatch, MemTable mem) {
        return null;
    }

    public static ByteBuffer contents(WriteBatch writeBatch) {
        return null;
    }

    public static long count(WriteBatch writeBatch) {
        return 0;
    }
}

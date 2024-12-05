package com.ldb.db;

public interface DB {

    boolean Write(WriteOptions options, WriteBatch updates);

}

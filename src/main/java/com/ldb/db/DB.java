package com.ldb.db;

public interface DB {

    Status write(WriteOptions options, WriteBatch updates);

}

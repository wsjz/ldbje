package com.ldb.log;

import com.ldb.db.Status;
import com.ldb.db.WritableFile;
import com.ldb.db.WriteBatch;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
public class Writer {
    WritableFile dest;

    public Writer(WritableFile dest) {
        this.dest = dest;
    }

    public Status addRecord(ByteBuffer contents) {
        return null;
    }
}

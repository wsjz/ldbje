package com.ldb.db;

import lombok.Data;

@Data
public class Writer {
    private WriteBatch batch;
    private boolean sync;
    private boolean done;
}

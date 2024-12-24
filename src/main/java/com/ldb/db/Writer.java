package com.ldb.db;

import lombok.Data;

import java.util.concurrent.locks.Condition;

@Data
public class Writer {
    private Condition cv;
    private WriteBatch batch;
    private boolean sync;
    private boolean done;
}

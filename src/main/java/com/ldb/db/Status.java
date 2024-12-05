package com.ldb.db;

import lombok.Getter;

@Getter
public class Status {
    private final RuntimeException ex;

    private Status(RuntimeException ex) {
        this.ex = ex;
    }

    public boolean isOk() {
        return ex == null;
    }

    public static Status of(RuntimeException ex) {
        return new Status(ex);
    }
}

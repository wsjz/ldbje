package com.ldb.db;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Status {
    private RuntimeException ex;

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

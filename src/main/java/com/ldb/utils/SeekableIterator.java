package com.ldb.utils;

public interface SeekableIterator<KEY> {
    boolean valid() ;
    KEY key() ;
    void next();
    void prev();
    void seek(KEY target);
    void seekToFirst();
    void seekToLast();
}

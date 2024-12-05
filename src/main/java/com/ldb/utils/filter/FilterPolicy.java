package com.ldb.utils.filter;

import java.util.List;

public interface FilterPolicy<T> {

    String name();

    T createFilter(List<byte[]> keys);

    boolean keyMayMatch(byte[] key, T filter);
}

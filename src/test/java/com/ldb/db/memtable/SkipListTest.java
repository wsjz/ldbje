package com.ldb.db.memtable;

import com.ldb.utils.SeekableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class SkipListTest {

    @Test
    public void testEmptyList() {
        SkipList<Integer> list = new SkipList<>();
        Assertions.assertFalse(list.contains(10));

        SeekableIterator<Integer> iter = list.iterator();
        Assertions.assertFalse(iter.valid());
        iter.seekToFirst();
        Assertions.assertFalse(iter.valid());
        iter.seek(100);
        Assertions.assertFalse(iter.valid());
        iter.seekToLast();
        Assertions.assertFalse(iter.valid());
    }

    @Test
    public void testInsertAndLookup() {
        int N = 2000;
        int R = 5000;
        Random rnd = new Random();
        TreeSet<Integer> keys = new TreeSet<>();
        SkipList<Integer> list = new SkipList<>();
        for (int i = 0; i < N; i++) {
            Integer key = rnd.nextInt(R) % R;
            if (keys.add(key)) {
                list.insert(key);
            }
        }

        for (int i = 0; i < R; i++) {
            if (list.contains(i)) {
                Assertions.assertTrue(keys.contains(i));
            } else {
                Assertions.assertFalse(keys.contains(i));
            }
        }

        // Simple iterator tests
        {
            SeekableIterator<Integer> iter = list.iterator();
            Assertions.assertFalse(iter.valid());

            Iterator<Integer> keyIter = keys.iterator();
            Integer firstKey = keyIter.next();
            iter.seek(0);
            Assertions.assertTrue(iter.valid());
            Assertions.assertEquals(iter.key(), firstKey);

            iter.seekToFirst();
            Assertions.assertTrue(iter.valid());
            Assertions.assertEquals(iter.key(), firstKey);

            Integer lastKey = keyIter.next();
            while (keyIter.hasNext()) {
                lastKey = keyIter.next();
            }
            iter.seekToLast();
            Assertions.assertTrue(iter.valid());
            Assertions.assertEquals(iter.key(), lastKey);
        }

        // Forward iteration test
        for (int i = 0; i < R; i++) {
            SeekableIterator<Integer> iter = list.iterator();
            iter.seek(i);
            // Compare against model iterator
            List<Integer> keyList = new ArrayList<>(keys);
            int lowerBound = keys.ceiling(i) == null ? keyList.size() : keyList.indexOf(keys.ceiling(i));
            Iterator<Integer> keyIter = keyList.listIterator(lowerBound);
            for (int j = 0; j < 3; j++) {
                if (!keyIter.hasNext()) {
                    Assertions.assertFalse(iter.valid());
                    break;
                } else {
                    Assertions.assertTrue(iter.valid());
                    Assertions.assertEquals(keyIter.next(), iter.key());
                    iter.next();
                }
            }
        }

        // Backward iteration test
        {
            SeekableIterator<Integer> iter = list.iterator();
            iter.seekToLast();
            ListIterator<Integer> iKeys = new ArrayList<>(keys).listIterator(keys.size());
            while (iKeys.hasPrevious()) {
                Assertions.assertTrue(iter.valid());
                Assertions.assertEquals(iKeys.previous(), iter.key());
                iter.prev();
            }
            Assertions.assertFalse(iter.valid());
        }
    }

    @Test
    public void testAccessConcurrency() throws InterruptedException, ExecutionException {
        ExecutorService executor = new ThreadPoolExecutor(64, 1000,
                10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        SkipList<Integer> list = new SkipList<>();
        List<Future<?>> writeFutures = new ArrayList<>();
        List<Future<?>> readFutures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            writeFutures.add(executor.submit(() -> writeTo(list)));
            readFutures.add(executor.submit(() -> readFrom(list)));
        }
        for (Future<?> writeFuture : writeFutures) {
            var ignored = writeFuture.get();
        }
        for (Future<?> readFuture : readFutures) {
            var ignored = readFuture.get();
        }
        SeekableIterator<Integer> iter = list.iterator();
        StringBuilder builder = new StringBuilder().append('[');
        while (iter.valid()) {
            builder.append(iter.key() + ',');
            iter.next();
        }
        String res = builder.append(']').toString();
        System.out.println(res);
//        if (executor.awaitTermination(30, TimeUnit.SECONDS)) {
//            executor.shutdownNow();
//        }
    }

    private void writeTo(SkipList<Integer> list) {
        int i = 0;
        while (i < 10) {
            if (!list.contains(i)) {
                list.insert(i);
            }
            i++;
        }
    }


    private void readFrom(SkipList<Integer> list) {

    }
}
package com.ldb.db;

import com.ldb.db.memtable.MemTable;
import lombok.Data;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Data
public class DBImpl implements DB {
    private long logFileNumber;
    private VersionSet versions;
    private MemTable mem;
    private MemTable imm;
    private volatile boolean hasImm = false;
    private Deque<Writer> writers = new ArrayDeque<>();
    private Lock lock = new ReentrantLock();
    private Condition wcv = lock.newCondition();
    private volatile Status bgError;

    public DBImpl(Options options, String name) {
    }

    @Override
    public boolean Write(WriteOptions options, WriteBatch updates) {
        Writer w = new Writer();
        w.setBatch(updates);
        w.setSync(options.isSync());
        w.setDone(false);

        lock.lock();
        try {
            writers.offer(w);
            while (!w.isDone() && w != writers.peekFirst()) {
                wcv.await();
            }
            if (w.isDone()) {
                return true;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }



        return false;
    }

    public void removeObsoleteFiles() {

    }

    public void maybeScheduleCompaction() {

    }

    public Optional<MemTable> getMemTable() {
        return Optional.ofNullable(mem);
    }

    public boolean recover(VersionEdit edit) {
        return false;
    }


    private void makeRoomForWrite(boolean force) {
        lock.lock();
        try {
            if (writers.isEmpty()) {
                throw new IllegalStateException("No writer available");
            }
        } finally {
            lock.unlock();
        }
        boolean allowDelay = !force;
        while (true) {
            if (!bgError.isOk()) {
                throw new IllegalStateException("background has an error: " + bgError.getEx());
            }
            if (allowDelay && versions.numLevelFiles(0) >= Config.kL0_SlowdownWritesTrigger) {
                try {
                    synchronized (this) {
                        this.wait(1000);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                allowDelay = false; // Do not delay a single write more than once
            } else if () {
            }


        }



    }
}

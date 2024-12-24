package com.ldb.db;

import com.ldb.Env;
import com.ldb.db.memtable.MemTable;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Data
public class DBImpl implements DB {
    private static final Logger LOG = LoggerFactory.getLogger(DBImpl.class);
    private final Options options;
    private final String dbName;
    private long logFileNumber;
    private VersionSet versions;
    private MemTable mem;
    private MemTable imm;
    private volatile AtomicBoolean hasImm = new AtomicBoolean(false);
    private Deque<Writer> writers = new ConcurrentLinkedDeque<>();
    private Lock lock = new ReentrantLock();
    private Condition wcv = lock.newCondition();
    private Condition backgroundWorkFinishedSignal = lock.newCondition();
    private volatile Status bgError;
    private WritableFile logFile;
    private Comparator internalComparator;
    private boolean backgroundCompactionScheduled;
    private AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private Object manualCompaction;
    private Env env;
    private com.ldb.log.Writer log;
    private WriteBatch tmpBatch;


    public DBImpl(Options options, String name) {
        this.options = options;
        this.dbName = name;
        this.logFileNumber = 0;
    }

    @Override
    public Status write(WriteOptions options, WriteBatch updates) {
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
                return Status.of(null);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
        try {
            Status status = makeRoomForWrite(updates == null);
            long lastSequence = versions.lastSequence();
            AtomicReference<Writer> lastWriter = new AtomicReference<>(w);
            if (status.isOk() && updates != null) {
                WriteBatch writeBatch = buildBatchGroup(lastWriter);
                WriteBatchInternal.setSequence(writeBatch, lastSequence + 1);
                lastSequence += WriteBatchInternal.count(writeBatch);

                // Add to log and apply to memtable.  We can release the lock
                // during this phase since &w is currently responsible for logging
                // and protects against concurrent loggers and concurrent writes
                // into mem_.
                status = log.addRecord(WriteBatchInternal.contents(writeBatch));
                boolean syncError = false;
                if (status.isOk() && options.isSync()) {
                    status = logFile.sync();
                    if (!status.isOk()) {
                        syncError = true;
                    }
                }
                if (status.isOk()) {
                    status = WriteBatchInternal.insertInto(writeBatch, mem);
                }
                lock.lock();
                if (syncError) {
                    // The state of the log file is indeterminate: the log record we
                    // just added may or may not show up when the DB is re-opened.
                    // So we force the DB into a mode where all future writes fail.
                    recordBackgroundError(status.getEx());
                }
                if (writeBatch == tmpBatch) tmpBatch.clear();

                versions.setLastSequence(lastSequence);
            }

            while (true) {
                Writer ready = writers.pollFirst();
                if (ready != null && ready != w) {
                    // ready.status = status;
                    ready.setDone(true);
                    ready.getCv().signal();
                }
                if (ready == lastWriter.getAcquire()) {
                    break;
                }
            }
            // Notify new head of write queue
            if (!writers.isEmpty()) {
                writers.peekFirst().getCv().signal();
            }
            return status;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private WriteBatch buildBatchGroup(AtomicReference<Writer> lastWriter) {
        return new WriteBatch();
    }

    public void removeObsoleteFiles() {

    }

    public Optional<MemTable> getMemTable() {
        return Optional.ofNullable(mem);
    }

    public boolean recover(VersionEdit edit) {
        return false;
    }


    private Status makeRoomForWrite(boolean force) throws InterruptedException {
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
                waitFor(1000);
                allowDelay = false; // Do not delay a single write more than once
            } else if (!force && mem.approximateMemoryUsage() <= options.writeBufferSize) {
                break;
            } else if (imm != null) {
                LOG.info("Current mem table full; waiting...\n");
                backgroundWorkFinishedSignal.await();
            } else if (versions.numLevelFiles(0) >= Config.kL0_StopWritesTrigger) {
                LOG.info("Too many L0 files; waiting...\n");
                backgroundWorkFinishedSignal.await();
            } else {
                assert(versions.prevLogNumber() == 0);
                long newLogNumber = versions.newFileNumber();
                WritableFile logFile;
                try {
                    logFile = env.newWritableFile(logFileName(dbName, newLogNumber));
                } catch (RuntimeException e) {
                    // Avoid chewing through file number space in a tight loop.
                    versions.reuseFileNumber(newLogNumber);
                    break;
                }
                try {
                    this.logFile.close();
                } catch (RuntimeException e) {
                    recordBackgroundError(e);
                }
                this.logFile = logFile;
                this.logFileNumber = newLogNumber;
                log = new com.ldb.log.Writer(logFile);
                imm = mem;
                hasImm.setRelease(true);
                mem = new MemTable(internalComparator);
                mem.ref();
                force = false;  // Do not force another compaction if have room
                maybeScheduleCompaction();
            }
        }
        return Status.of(null);
    }

    public void maybeScheduleCompaction() {
        if (backgroundCompactionScheduled) {
            // Already scheduled
        } else if (shuttingDown.getAcquire()) {
            // DB is being deleted; no more background compactions
        } else if (!bgError.isOk()) {
            // Already got an error; no more changes
        } else if (imm == null && manualCompaction == null &&
                !versions.needsCompaction()) {
            // No work to be done
        } else {
            backgroundCompactionScheduled = true;
            env.schedule(this::backgroundCall);
        }
    }

    private void backgroundCall() {

    }

    private void recordBackgroundError(RuntimeException e) {
        if (bgError.isOk()) {
            bgError.setEx(e);
            backgroundWorkFinishedSignal.signalAll();
        }
    }

    public static WritableFile logFileName(String dbName, long newLogNumber) {
        return null;
    }

    private void waitFor(long timeout) throws InterruptedException {
        synchronized (this) {
            this.wait(timeout);
        }
    }
}

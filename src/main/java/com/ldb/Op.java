package com.ldb;

import com.ldb.db.DB;
import com.ldb.db.DBImpl;
import com.ldb.db.Options;
import com.ldb.db.VersionEdit;
import com.ldb.db.WritableFile;
import com.ldb.db.memtable.MemTable;
import com.ldb.log.Writer;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Op {

    private static final Lock lock = new ReentrantLock();

    public static DB Open(Options options, String name) {
        DBImpl db = new DBImpl(options, name);
        lock.lock();
        try {
            VersionEdit edit = new VersionEdit();
            boolean saveManifest = db.recover(edit);
            if (db.getMem() == null) {
                // Create new log and a corresponding memtable.
                long newLogNumber = db.getVersions().newFileNumber();
                try {
                    WritableFile logfile = options.getEnv()
                            .newWritableFile(DBImpl.logFileName(db.getDbName(), newLogNumber));
                    edit.setLogNumber(newLogNumber);
                    db.setLogFile(logfile);
                    db.setLogFileNumber(newLogNumber);
                    db.setLog(new Writer(logfile));
                    db.setMem(new MemTable(db.getInternalComparator()));
                    db.getMem().ref();
                } catch (RuntimeException e) {
                    throw new RuntimeException(e);
                }
            }
            if (saveManifest) {
                edit.setPrevLogNumber(0);
                edit.setLogNumber(db.getLogFileNumber());
                db.getVersions().logAndApply(edit);
            }
            db.removeObsoleteFiles();
            db.maybeScheduleCompaction();
        } finally {
            lock.unlock();
        }
        if (db.getMemTable().isPresent()) {
            return db;
        }
        throw new RuntimeException("Can't open DB");
    }
}

package com.ldb.db;

public class VersionSet {
    Version current;
    private long prevLogNumber;

    public void logAndApply(VersionEdit edit) {
    }

    public int numLevelFiles(int level) {
        assert(level >= 0);
        assert(level < Config.kNumLevels);
        return current.getFiles().get(level).size();
    }

    public long prevLogNumber() {
        return prevLogNumber;
    }

    public long newFileNumber() {
        return 0;
    }

    public void reuseFileNumber(long newLogNumber) {

    }

    public boolean needsCompaction() {
        return false;
    }

    public long lastSequence() {
        return 0;
    }

    public void setLastSequence(long lastSequence) {

    }
}

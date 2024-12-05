package com.ldb.db;

public class VersionSet {
    Version current;

    public void logAndApply(VersionEdit edit) {
    }

    public int numLevelFiles(int level) {
        assert(level >= 0);
        assert(level < Config.kNumLevels);
        return current.getFiles().get(level).size();
    }
}

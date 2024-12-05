package com.ldb.db;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class Version {
    List<FileMetaData> files = new ArrayList<>(Config.kNumLevels);
}

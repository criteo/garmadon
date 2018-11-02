package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.reader.Offset;

import java.time.LocalDateTime;

public class FixedFileNamer implements FileNamer {
    private String fileName;

    public FixedFileNamer(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public String buildPath(LocalDateTime dayStartTime, Offset offset) {
        return fileName;
    }
}

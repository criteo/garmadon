package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.reader.Offset;

import java.time.LocalDateTime;

/**
 * Always return the same file name when asked for it
 */
public class FixedFileNamer implements FileNamer {
    private final String fileName;

    /**
     * @param fileName  The name to return everytime
     */
    public FixedFileNamer(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public String buildPath(LocalDateTime dayStartTime, Offset offset) {
        return fileName;
    }
}

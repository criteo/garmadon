package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.reader.Offset;

import java.time.LocalDateTime;

/**
 * Always return the same file name when asked for it
 */
public class FixedOffsetComputer implements OffsetComputer {
    private final String fileName;
    private final long offset;

    /**
     * @param fileName  The name to return everytime
     */
    public FixedOffsetComputer(String fileName, long offset) {
        this.fileName = fileName;
        this.offset = offset;
    }

    @Override
    public long computeOffset(int partitionId) {
        return offset;
    }

    @Override
    public String computePath(LocalDateTime time, Offset offset) {
        return fileName;
    }
}

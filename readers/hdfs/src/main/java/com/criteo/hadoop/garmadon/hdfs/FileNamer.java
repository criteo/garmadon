package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.reader.Offset;

import java.time.LocalDateTime;

public interface FileNamer {
    /**
     * Provide a path based on a time and offset
     *
     * @param dayStartTime
     * @param offset
     * @return
     */
    String buildPath(LocalDateTime dayStartTime, Offset offset);
}

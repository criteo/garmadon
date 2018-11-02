package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.reader.Offset;

import java.time.LocalDateTime;

public interface FileNamer {
    String buildPath(LocalDateTime dayStartTime, Offset offset);
}
